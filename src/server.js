import express from 'express'
import cors from 'cors'
import pg from 'pg'
import multer from 'multer'
import { S3Client, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const { Pool } = pg
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const app = express()
const APP_BATCH_VERSION = '0.0.18-53-public-ladies-include-inactive'
const port = Number(process.env.PORT || 5260)
const dataDir = path.resolve(__dirname, '../data')
const ladiesFile = path.join(dataDir, 'ladies.json')

const databaseUrl = process.env.DATABASE_URL || ''
const r2AccountId = process.env.R2_ACCOUNT_ID || ''
const r2AccessKeyId = process.env.R2_ACCESS_KEY_ID || ''
const r2SecretAccessKey = process.env.R2_SECRET_ACCESS_KEY || ''
const r2BucketName = process.env.R2_BUCKET_NAME || ''
const r2PublicBaseUrl = (process.env.R2_PUBLIC_BASE_URL || '').replace(/\/+$/, '')
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 200 * 1024 * 1024
  }
})

let pool = null

function getPool() {
  if (!databaseUrl) return null

  if (!pool) {
    pool = new Pool({
      connectionString: databaseUrl,
      ssl: databaseUrl.includes('localhost') || databaseUrl.includes('127.0.0.1')
        ? false
        : { rejectUnauthorized: false }
    })
  }

  return pool
}


function getR2Client() {
  if (!r2AccountId || !r2AccessKeyId || !r2SecretAccessKey || !r2BucketName || !r2PublicBaseUrl) {
    throw new Error('R2 尚未設定完整。請設定 R2_ACCOUNT_ID、R2_ACCESS_KEY_ID、R2_SECRET_ACCESS_KEY、R2_BUCKET_NAME、R2_PUBLIC_BASE_URL。')
  }

  return new S3Client({
    region: 'auto',
    endpoint: `https://${r2AccountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: r2AccessKeyId,
      secretAccessKey: r2SecretAccessKey
    }
  })
}

function sanitizeFileName(name) {
  return String(name || 'file')
    .replace(/[^\w.\-]+/g, '-')
    .replace(/-+/g, '-')
    .slice(0, 120)
}

function getMediaTypeFromFile(file, fallbackType) {
  if (fallbackType === 'video' || fallbackType === 'image') return fallbackType

  if (file?.mimetype?.startsWith('video/')) return 'video'
  return 'image'
}


app.use(cors())
app.use(express.json({ limit: '20mb' }))

async function ensureDataFile() {
  await mkdir(dataDir, { recursive: true })

  try {
    await readFile(ladiesFile, 'utf8')
  } catch {
    await writeFile(ladiesFile, JSON.stringify({ items: [], updatedAt: null }, null, 2), 'utf8')
  }
}

async function readLadies() {
  await ensureDataFile()
  const raw = await readFile(ladiesFile, 'utf8')
  return JSON.parse(raw || '{"items":[]}')
}

async function writeLadies(items) {
  await ensureDataFile()
  const payload = {
    updatedAt: new Date().toISOString(),
    count: items.length,
    items
  }

  await writeFile(ladiesFile, JSON.stringify(payload, null, 2), 'utf8')
  return payload
}

async function ensureDatabaseTables() {
  const db = getPool()
  if (!db) {
    throw new Error('尚未設定 DATABASE_URL。請先在 backend/.env 或部署環境變數設定 Supabase PostgreSQL 連線字串。')
  }

  await db.query(`
    create table if not exists ladies (
      id bigserial primary key,
      country text not null default '',
      name text not null default '',
      height integer,
      weight integer,
      cup text not null default '',
      age integer,
      raw_text text not null default '',
      is_active boolean not null default true,
      sort_order integer not null default 0,
      imported_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
  `)

  await db.query(`
    create table if not exists lady_price_plans (
      id bigserial primary key,
      lady_id bigint not null references ladies(id) on delete cascade,
      price_text text not null default '',
      price integer not null default 0,
      minutes integer not null default 0,
      sessions numeric not null default 1,
      sort_order integer not null default 0
    );
  `)

  await db.query(`
    create table if not exists lady_services (
      id bigserial primary key,
      lady_id bigint not null references ladies(id) on delete cascade,
      service_name text not null default '',
      sort_order integer not null default 0
    );
  `)

  await db.query(`create index if not exists idx_ladies_country_name on ladies(country, name);`)
  await db.query(`create index if not exists idx_lady_price_plans_lady_id on lady_price_plans(lady_id);`)
  await db.query(`create index if not exists idx_lady_services_lady_id on lady_services(lady_id);`)

  await db.query(`
    create table if not exists lady_media (
      id bigserial primary key,
      lady_id bigint not null references ladies(id) on delete cascade,
      media_type text not null default 'image',
      url text not null default '',
      object_key text not null default '',
      note text not null default '',
      sort_order integer not null default 0,
      uploaded_at timestamptz not null default now()
    );
  `)

  await db.query(`create index if not exists idx_lady_media_lady_id on lady_media(lady_id);`)

}

function normalizeItems(body) {
  const items = Array.isArray(body?.items) ? body.items : []

  return items.map((item, index) => ({
    sourceIndex: item.sourceIndex || index + 1,
    country: String(item.country || '').trim(),
    name: String(item.name || '').trim(),
    body: item.body || {},
    pricePlans: Array.isArray(item.pricePlans) ? item.pricePlans : [],
    services: Array.isArray(item.services) ? item.services : [],
    rawText: String(item.rawText || '').trim()
  }))
}

function makeLadyMergeKey(item) {
  const country = String(item?.country || '').trim()
  const name = String(item?.name || '').trim()
  return `${country}__${name}`.toLowerCase()
}

function mergeLocalLadies(existingItems, incomingItems) {
  const now = new Date().toISOString()
  const merged = []
  const indexByKey = new Map()

  ;(Array.isArray(existingItems) ? existingItems : []).forEach((item, index) => {
    const key = makeLadyMergeKey(item)
    if (!key || key === '__') return

    indexByKey.set(key, merged.length)
    merged.push({
      ...item,
      sourceIndex: item.sourceIndex || index + 1
    })
  })

  let createdCount = 0
  let updatedCount = 0
  let skippedCount = 0

  ;(Array.isArray(incomingItems) ? incomingItems : []).forEach((item, index) => {
    const key = makeLadyMergeKey(item)
    if (!item?.name || !key || key === '__') {
      skippedCount += 1
      return
    }

    const nextItem = {
      ...item,
      id: item.id || `${Date.now()}-${index + 1}`,
      importedAt: item.importedAt || now,
      updatedAt: now
    }

    if (indexByKey.has(key)) {
      const existingIndex = indexByKey.get(key)
      merged[existingIndex] = {
        ...merged[existingIndex],
        ...nextItem,
        id: merged[existingIndex].id || nextItem.id,
        importedAt: merged[existingIndex].importedAt || nextItem.importedAt,
        updatedAt: now
      }
      updatedCount += 1
    } else {
      indexByKey.set(key, merged.length)
      merged.push(nextItem)
      createdCount += 1
    }
  })

  return {
    items: merged,
    createdCount,
    updatedCount,
    skippedCount
  }
}


app.get('/api/health', async (_req, res) => {
  res.json({
    ok: true,
    message: 'auto-document-converter local API is running',
    time: new Date().toISOString()
  })
})


app.get('/api/version', (_req, res) => {
  res.json({
    ok: true,
    version: APP_BATCH_VERSION,
    batch: '018-15',
    importMode: 'append_upsert_keep_existing',
    destructiveImportDisabled: true
  })
})

app.get('/api/db/health', async (_req, res) => {
  try {
    await ensureDatabaseTables()
    const result = await getPool().query('select now() as now')

    res.json({
      ok: true,
      message: 'Supabase PostgreSQL connection is ready',
      time: result.rows[0]?.now || new Date().toISOString()
    })
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  }
})

app.get('/api/ladies', async (_req, res) => {
  const data = await readLadies()
  res.json(data)
})

app.post('/api/ladies/import', async (req, res) => {
  const normalized = normalizeItems(req.body)

  if (!normalized.length) {
    return res.status(400).json({
      ok: false,
      message: '沒有收到 items 資料。請先確認文件3有內容，文件4 JSON 有產生。'
    })
  }

  const existing = await readLadies()
  const beforeCount = Array.isArray(existing.items) ? existing.items.length : 0
  const merged = mergeLocalLadies(existing.items, normalized)
  const saved = await writeLadies(merged.items)
  const afterCount = saved.count

  res.json({
    ok: true,
    message: `已匯入本機 JSON：匯入前 ${beforeCount} 筆，新增 ${merged.createdCount} 筆，更新 ${merged.updatedCount} 筆，略過 ${merged.skippedCount} 筆，匯入後 ${afterCount} 筆。`,
    count: normalized.length,
    beforeCount,
    afterCount,
    createdCount: merged.createdCount,
    updatedCount: merged.updatedCount,
    skippedCount: merged.skippedCount,
    mode: 'local_append_upsert_keep_existing',
    version: APP_BATCH_VERSION,
    dataFile: ladiesFile
  })
})

app.post('/api/ladies/import-db', async (req, res) => {
  const normalized = normalizeItems(req.body)

  if (!normalized.length) {
    return res.status(400).json({
      ok: false,
      message: '沒有收到 items 資料。請先確認文件3有內容，文件4 JSON 有產生。'
    })
  }

  const db = getPool()
  if (!db) {
    return res.status(400).json({
      ok: false,
      message: '尚未設定 DATABASE_URL。請先設定 Supabase PostgreSQL 連線字串。'
    })
  }

  const client = await db.connect()

  try {
    await ensureDatabaseTables()
    await client.query('begin')

    const beforeCountResult = await client.query('select count(*)::int as count from ladies')
    const beforeCount = Number(beforeCountResult.rows[0]?.count || 0)

    let createdCount = 0
    let updatedCount = 0
    let skippedCount = 0

    const maxSortResult = await client.query('select coalesce(max(sort_order), 0) as max_sort from ladies')
    let nextSortOrder = Number(maxSortResult.rows[0]?.max_sort || 0) + 1

    for (const item of normalized) {
      const name = String(item.name || '').trim()
      const country = String(item.country || '').trim()
      const body = item.body || {}

      if (!name) {
        skippedCount += 1
        continue
      }

      // 嚴格用「國籍 + 名稱」判斷同一位，不能因為新批次而清空舊批次。
      const existingLadyResult = await client.query(
        `
          select id
          from ladies
          where lower(trim(coalesce(country, ''))) = lower(trim($1))
            and lower(trim(coalesce(name, ''))) = lower(trim($2))
          order by id asc
          limit 1
        `,
        [country, name]
      )

      let ladyId = existingLadyResult.rows[0]?.id

      if (ladyId) {
        await client.query(
          `
            update ladies
            set
              height = $1,
              weight = $2,
              cup = $3,
              age = $4,
              raw_text = $5,
              updated_at = now()
            where id = $6
          `,
          [
            body.height ?? null,
            body.weight ?? null,
            body.cup || '',
            body.age ?? null,
            item.rawText,
            ladyId
          ]
        )

        updatedCount += 1
      } else {
        const insertedLady = await client.query(
          `
            insert into ladies (
              country,
              name,
              height,
              weight,
              cup,
              age,
              raw_text,
              is_active,
              sort_order,
              imported_at,
              updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,true,$8,now(),now())
            returning id
          `,
          [
            country,
            name,
            body.height ?? null,
            body.weight ?? null,
            body.cup || '',
            body.age ?? null,
            item.rawText,
            nextSortOrder
          ]
        )

        ladyId = insertedLady.rows[0].id
        nextSortOrder += 1
        createdCount += 1
      }

      // 只更新這位小姐自己的方案與服務；不碰其他小姐。
      await client.query('delete from lady_price_plans where lady_id = $1', [ladyId])
      await client.query('delete from lady_services where lady_id = $1', [ladyId])

      for (const [priceIndex, plan] of item.pricePlans.entries()) {
        await client.query(
          `
            insert into lady_price_plans (
              lady_id,
              price_text,
              price,
              minutes,
              sessions,
              sort_order
            )
            values ($1,$2,$3,$4,$5,$6)
          `,
          [
            ladyId,
            plan.priceText || '',
            Number(plan.price || 0),
            Number(plan.minutes || 0),
            Number(plan.sessions || 1),
            priceIndex + 1
          ]
        )
      }

      for (const [serviceIndex, serviceName] of item.services.entries()) {
        const cleanServiceName = String(serviceName || '').trim()
        if (!cleanServiceName) continue

        await client.query(
          `
            insert into lady_services (
              lady_id,
              service_name,
              sort_order
            )
            values ($1,$2,$3)
          `,
          [
            ladyId,
            cleanServiceName,
            serviceIndex + 1
          ]
        )
      }
    }

    const afterCountResult = await client.query('select count(*)::int as count from ladies')
    const afterCount = Number(afterCountResult.rows[0]?.count || 0)

    if (afterCount < beforeCount) {
      throw new Error(`安全檢查失敗：匯入後筆數 ${afterCount} 小於匯入前 ${beforeCount}，已取消本次匯入。`)
    }

    await client.query('commit')

    res.json({
      ok: true,
      message: `已同步 Supabase：匯入前 ${beforeCount} 筆，新增 ${createdCount} 筆，更新 ${updatedCount} 筆，略過 ${skippedCount} 筆，匯入後 ${afterCount} 筆。舊小姐與媒體已保留。`,
      count: normalized.length,
      beforeCount,
      afterCount,
      createdCount,
      updatedCount,
      skippedCount,
      mode: 'db_append_upsert_keep_existing_safety_checked',
      version: APP_BATCH_VERSION
    })
  } catch (error) {
    await client.query('rollback')

    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  } finally {
    client.release()
  }
})

app.get('/api/ladies/db', async (_req, res) => {
  try {
    await ensureDatabaseTables()

    const ladies = await getPool().query(`
      select *
      from ladies
      order by sort_order asc, id asc
    `)

    res.json({
      ok: true,
      count: ladies.rows.length,
      items: ladies.rows
    })
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  }
})



app.patch('/api/ladies/:id', async (req, res) => {
  const ladyId = Number(req.params?.id || 0)

  if (!ladyId) {
    return res.status(400).json({
      ok: false,
      message: '缺少 lady id。'
    })
  }

  const body = req.body || {}
  const hasIsActive = Object.prototype.hasOwnProperty.call(body, 'isActive')
    || Object.prototype.hasOwnProperty.call(body, 'is_active')
    || Object.prototype.hasOwnProperty.call(body, 'status')

  const nextIsActive = hasIsActive
    ? (
      body.isActive === true
      || body.is_active === true
      || body.status === 'published'
      || body.status === 'active'
      || body.status === '上架'
    )
    : null

  try {
    await ensureDatabaseTables()
    const db = getPool()

    if (db) {
      const exists = await db.query('select * from ladies where id = $1 limit 1', [ladyId])
      if (!exists.rows.length) {
        return res.status(404).json({
          ok: false,
          message: '找不到指定小姐資料。'
        })
      }

      const current = exists.rows[0]
      const nextName = body.name !== undefined ? String(body.name || '').trim() : current.name
      const nextCountry = body.country !== undefined || body.nationality !== undefined
        ? String(body.country || body.nationality || '').trim()
        : current.country
      const nextHeight = body.height !== undefined && body.height !== ''
        ? Number(body.height)
        : current.height
      const nextWeight = body.weight !== undefined && body.weight !== ''
        ? Number(body.weight)
        : current.weight
      const nextCup = body.cup !== undefined ? String(body.cup || '').trim() : current.cup
      const nextAge = body.age !== undefined && body.age !== ''
        ? Number(body.age)
        : current.age
      const nextRawText = body.rawText !== undefined ? String(body.rawText || '') : current.raw_text
      const activeValue = hasIsActive ? nextIsActive : current.is_active

      const updated = await db.query(
        `
          update ladies
          set
            name = $1,
            country = $2,
            height = $3,
            weight = $4,
            cup = $5,
            age = $6,
            raw_text = $7,
            is_active = $8,
            updated_at = now()
          where id = $9
          returning *
        `,
        [
          nextName,
          nextCountry,
          Number.isFinite(nextHeight) ? nextHeight : null,
          Number.isFinite(nextWeight) ? nextWeight : null,
          nextCup,
          Number.isFinite(nextAge) ? nextAge : null,
          nextRawText,
          activeValue,
          ladyId
        ]
      )

      const item = updated.rows[0]

      return res.json({
        ok: true,
        message: activeValue ? '小姐資料已更新並設為上架。' : '小姐資料已更新並設為下架。',
        item: {
          ...item,
          isActive: item.is_active,
          status: item.is_active ? 'published' : 'unpublished'
        }
      })
    }

    const localData = await readLadies()
    const items = Array.isArray(localData.items) ? localData.items : []
    const index = items.findIndex(item => Number(item.id) === ladyId || String(item.id) === String(ladyId))

    if (index < 0) {
      return res.status(404).json({
        ok: false,
        message: '找不到指定小姐資料。'
      })
    }

    const current = items[index]
    const activeValue = hasIsActive ? nextIsActive : (current.isActive ?? current.is_active ?? true)

    items[index] = {
      ...current,
      name: body.name !== undefined ? String(body.name || '').trim() : current.name,
      country: body.country !== undefined || body.nationality !== undefined
        ? String(body.country || body.nationality || '').trim()
        : current.country,
      body: {
        ...(current.body || {}),
        height: body.height !== undefined ? body.height : current.body?.height,
        weight: body.weight !== undefined ? body.weight : current.body?.weight,
        cup: body.cup !== undefined ? body.cup : current.body?.cup,
        age: body.age !== undefined ? body.age : current.body?.age
      },
      rawText: body.rawText !== undefined ? String(body.rawText || '') : current.rawText,
      isActive: activeValue,
      is_active: activeValue,
      status: activeValue ? 'published' : 'unpublished',
      updatedAt: new Date().toISOString()
    }

    const saved = await writeLadies(items)

    return res.json({
      ok: true,
      message: activeValue ? '小姐資料已更新並設為上架。' : '小姐資料已更新並設為下架。',
      item: items[index],
      count: saved.count
    })
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  }
})


app.post('/api/ladies/media/upload', upload.single('file'), async (req, res) => {
  try {
    await ensureDatabaseTables()

    const db = getPool()
    if (!db) {
      return res.status(400).json({
        ok: false,
        message: '尚未設定 DATABASE_URL，無法綁定媒體到小姐資料。'
      })
    }

    const file = req.file
    const ladyId = Number(req.body?.ladyId || 0)
    const mediaType = getMediaTypeFromFile(file, req.body?.mediaType)
    const note = String(req.body?.note || '').trim()

    if (!ladyId) {
      return res.status(400).json({
        ok: false,
        message: '缺少 ladyId。'
      })
    }

    if (!file) {
      return res.status(400).json({
        ok: false,
        message: '沒有收到檔案。'
      })
    }

    const exists = await db.query('select id from ladies where id = $1 limit 1', [ladyId])
    if (!exists.rows.length) {
      return res.status(404).json({
        ok: false,
        message: '找不到指定小姐資料。'
      })
    }

    const r2 = getR2Client()
    const safeName = sanitizeFileName(file.originalname)
    const objectKey = `ladies/${ladyId}/${Date.now()}-${safeName}`

    await r2.send(new PutObjectCommand({
      Bucket: r2BucketName,
      Key: objectKey,
      Body: file.buffer,
      ContentType: file.mimetype || 'application/octet-stream'
    }))

    const url = `${r2PublicBaseUrl}/${objectKey}`

    const sortResult = await db.query(
      'select coalesce(max(sort_order), 0) + 1 as next_sort from lady_media where lady_id = $1',
      [ladyId]
    )
    const nextSort = Number(sortResult.rows[0]?.next_sort || 1)

    const inserted = await db.query(
      `
        insert into lady_media (
          lady_id,
          media_type,
          url,
          object_key,
          note,
          sort_order,
          uploaded_at
        )
        values ($1,$2,$3,$4,$5,$6,now())
        returning *
      `,
      [
        ladyId,
        mediaType,
        url,
        objectKey,
        note,
        nextSort
      ]
    )

    res.json({
      ok: true,
      message: '媒體已上傳到 Cloudflare R2 並綁定小姐資料。',
      url,
      item: inserted.rows[0]
    })
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  }
})


app.delete('/api/ladies/media/:mediaId', async (req, res) => {
  try {
    await ensureDatabaseTables()

    const db = getPool()
    if (!db) {
      return res.status(400).json({
        ok: false,
        message: '尚未設定 DATABASE_URL，無法刪除媒體。'
      })
    }

    const mediaId = Number(req.params?.mediaId || 0)
    if (!mediaId) {
      return res.status(400).json({
        ok: false,
        message: '缺少 mediaId。'
      })
    }

    const mediaResult = await db.query('select * from lady_media where id = $1 limit 1', [mediaId])
    const mediaItem = mediaResult.rows[0]

    if (!mediaItem) {
      return res.status(404).json({
        ok: false,
        message: '找不到指定媒體。'
      })
    }

    if (mediaItem.object_key) {
      const r2 = getR2Client()
      await r2.send(new DeleteObjectCommand({
        Bucket: r2BucketName,
        Key: mediaItem.object_key
      }))
    }

    await db.query('delete from lady_media where id = $1', [mediaId])

    res.json({
      ok: true,
      message: '媒體已從 Cloudflare R2 與 Supabase 綁定資料刪除。',
      mediaId,
      ladyId: mediaItem.lady_id
    })
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  }
})


app.get('/api/public/ladies', async (req, res) => {
  try {
    await ensureDatabaseTables()

    const db = getPool()
    const includeInactive = ['1', 'true', 'yes', 'all'].includes(String(req.query?.includeInactive || '').toLowerCase())

    const ladiesResult = await db.query(`
      select
        id,
        country,
        name,
        height,
        weight,
        cup,
        age,
        raw_text,
        is_active,
        sort_order,
        imported_at,
        updated_at
      from ladies
      ${includeInactive ? '' : 'where is_active = true'}
      order by sort_order asc, id asc
    `)

    const ladyIds = ladiesResult.rows.map(item => item.id)

    if (!ladyIds.length) {
      return res.json({
        ok: true,
        count: 0,
        includeInactive,
        items: []
      })
    }

    const pricesResult = await db.query(
      `
        select
          id,
          lady_id,
          price_text,
          price,
          minutes,
          sessions,
          sort_order
        from lady_price_plans
        where lady_id = any($1::bigint[])
        order by lady_id asc, sort_order asc, id asc
      `,
      [ladyIds]
    )

    const servicesResult = await db.query(
      `
        select
          id,
          lady_id,
          service_name,
          sort_order
        from lady_services
        where lady_id = any($1::bigint[])
        order by lady_id asc, sort_order asc, id asc
      `,
      [ladyIds]
    )

    const mediaResult = await db.query(
      `
        select
          id,
          lady_id,
          media_type,
          url,
          object_key,
          note,
          sort_order,
          uploaded_at
        from lady_media
        where lady_id = any($1::bigint[])
        order by lady_id asc, sort_order asc, id asc
      `,
      [ladyIds]
    )

    const pricesByLadyId = new Map()
    pricesResult.rows.forEach(item => {
      const list = pricesByLadyId.get(item.lady_id) || []
      list.push({
        id: item.id,
        priceText: item.price_text,
        price: item.price,
        minutes: item.minutes,
        sessions: Number(item.sessions)
      })
      pricesByLadyId.set(item.lady_id, list)
    })

    const servicesByLadyId = new Map()
    servicesResult.rows.forEach(item => {
      const list = servicesByLadyId.get(item.lady_id) || []
      list.push({
        id: item.id,
        serviceName: item.service_name
      })
      servicesByLadyId.set(item.lady_id, list)
    })

    const mediaByLadyId = new Map()
    mediaResult.rows.forEach(item => {
      const list = mediaByLadyId.get(item.lady_id) || []
      list.push({
        id: item.id,
        mediaType: item.media_type,
        url: item.url,
        objectKey: item.object_key,
        note: item.note,
        uploadedAt: item.uploaded_at
      })
      mediaByLadyId.set(item.lady_id, list)
    })

    const items = ladiesResult.rows.map(item => ({
      id: item.id,
      country: item.country,
      name: item.name,
      height: item.height,
      weight: item.weight,
      cup: item.cup,
      age: item.age,
      rawText: item.raw_text,
      isActive: item.is_active,
      is_active: item.is_active,
      status: item.is_active ? 'published' : 'unpublished',
      sortOrder: item.sort_order,
      importedAt: item.imported_at,
      updatedAt: item.updated_at,
      pricePlans: pricesByLadyId.get(item.id) || [],
      services: servicesByLadyId.get(item.id) || [],
      media: mediaByLadyId.get(item.id) || []
    }))

    res.json({
      ok: true,
      count: items.length,
      includeInactive,
      items
    })
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  }
})

app.listen(port, async () => {
  await ensureDataFile()
  console.log(`Auto Document Converter API running at http://localhost:${port}`)
  if (databaseUrl) {
    console.log('DATABASE_URL detected. Supabase PostgreSQL mode is available.')
  } else {
    console.log('DATABASE_URL not set. Local JSON mode only.')
  }
})
