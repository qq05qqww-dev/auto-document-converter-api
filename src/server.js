import express from 'express'
import cors from 'cors'
import pg from 'pg'
import multer from 'multer'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const { Pool } = pg
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const app = express()
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

app.get('/api/health', async (_req, res) => {
  res.json({
    ok: true,
    message: 'auto-document-converter local API is running',
    time: new Date().toISOString()
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

  const saved = await writeLadies(normalized.map((item, index) => ({
    id: `${Date.now()}-${index + 1}`,
    ...item,
    importedAt: new Date().toISOString()
  })))

  res.json({
    ok: true,
    message: '已匯入本機 JSON 檔。',
    count: saved.count,
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

    await client.query('delete from ladies')

    for (const [index, item] of normalized.entries()) {
      const body = item.body || {}
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
            sort_order,
            imported_at,
            updated_at
          )
          values ($1,$2,$3,$4,$5,$6,$7,$8,now(),now())
          returning id
        `,
        [
          item.country,
          item.name,
          body.height ?? null,
          body.weight ?? null,
          body.cup || '',
          body.age ?? null,
          item.rawText,
          index + 1
        ]
      )

      const ladyId = insertedLady.rows[0].id

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
            String(serviceName || '').trim(),
            serviceIndex + 1
          ]
        )
      }
    }

    await client.query('commit')

    res.json({
      ok: true,
      message: '已匯入 Supabase PostgreSQL。',
      count: normalized.length
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




function parseTextLines(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => String(item || '').trim())
      .filter(Boolean)
  }

  return String(value || '')
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean)
}

function parsePricePlanText(priceText, sortOrder) {
  const text = String(priceText || '').trim()
  const priceMatch = text.match(/(\d+(?:\.\d+)?)\s*K/i)
  const rawPriceMatch = text.match(/^(\d{3,5})\s*\//)
  const minutesMatch = text.match(/\/\s*(\d+)\s*\//)
  const sessionMatch = text.match(/\/\s*[^/]*?(\d+(?:\.\d+)?|0\.5)\s*S?/i)

  let price = null
  if (priceMatch) {
    price = Math.round(Number(priceMatch[1]) * 1000)
  } else if (rawPriceMatch) {
    price = Number(rawPriceMatch[1])
  }

  return {
    priceText: text,
    price,
    minutes: minutesMatch ? Number(minutesMatch[1]) : null,
    sessions: sessionMatch ? Number(sessionMatch[1]) : 1,
    sortOrder
  }
}

app.patch('/api/ladies/:id', async (req, res) => {
  const ladyId = Number(req.params.id || 0)

  if (!ladyId) {
    return res.status(400).json({
      ok: false,
      message: '缺少有效的 lady id。'
    })
  }

  const db = getPool()
  if (!db) {
    return res.status(400).json({
      ok: false,
      message: '尚未設定 DATABASE_URL，無法更新 Supabase 小姐資料。'
    })
  }

  const body = req.body || {}
  const name = String(body.name || '').trim()
  const country = String(body.country || body.nationality || body.city || '').trim()
  const cup = String(body.cup || '').trim()
  const rawText = String(body.rawText || body.raw_text || body.description || '').trim()
  const priceLines = parseTextLines(body.pricePlansText || body.plansText || body.prices || body.plans)
  const serviceLines = parseTextLines(body.servicesText || body.services || body.serviceList)

  if (!name) {
    return res.status(400).json({
      ok: false,
      message: '請先輸入姓名。'
    })
  }

  const client = await db.connect()

  try {
    await ensureDatabaseTables()
    await client.query('begin')

    const updatedLady = await client.query(
      `
        update ladies
        set
          country = $1,
          name = $2,
          height = $3,
          weight = $4,
          cup = $5,
          age = $6,
          raw_text = coalesce(nullif($7, ''), raw_text),
          is_active = coalesce($8, is_active),
          updated_at = now()
        where id = $9
        returning *
      `,
      [
        country,
        name,
        body.height === '' || body.height === null || body.height === undefined ? null : Number(body.height),
        body.weight === '' || body.weight === null || body.weight === undefined ? null : Number(body.weight),
        cup,
        body.age === '' || body.age === null || body.age === undefined ? null : Number(body.age),
        rawText,
        typeof body.isActive === 'boolean' ? body.isActive : null,
        ladyId
      ]
    )

    if (!updatedLady.rows.length) {
      await client.query('rollback')
      return res.status(404).json({
        ok: false,
        message: '找不到要更新的小姐資料。'
      })
    }

    await client.query('delete from lady_price_plans where lady_id = $1', [ladyId])
    for (const [index, line] of priceLines.entries()) {
      const plan = parsePricePlanText(line, index + 1)
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
          values ($1, $2, $3, $4, $5, $6)
        `,
        [ladyId, plan.priceText, plan.price, plan.minutes, plan.sessions, plan.sortOrder]
      )
    }

    await client.query('delete from lady_services where lady_id = $1', [ladyId])
    for (const [index, serviceName] of serviceLines.entries()) {
      await client.query(
        `
          insert into lady_services (
            lady_id,
            service_name,
            sort_order
          )
          values ($1, $2, $3)
        `,
        [ladyId, serviceName, index + 1]
      )
    }

    await client.query('commit')

    return res.json({
      ok: true,
      message: '自動更新資料已同步更新到 Supabase。',
      item: updatedLady.rows[0],
      pricePlanCount: priceLines.length,
      serviceCount: serviceLines.length
    })
  } catch (error) {
    await client.query('rollback')
    console.error('PATCH /api/ladies/:id failed:', error)
    return res.status(500).json({
      ok: false,
      message: error.message || String(error)
    })
  } finally {
    client.release()
  }
})

app.get('/api/public/ladies', async (req, res) => {
  try {
    const includeInactive = String(req.query.includeInactive || '') === '1'
    await ensureDatabaseTables()

    const db = getPool()

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
      where ($1::boolean = true or is_active = true)
      order by sort_order asc, id asc
    `, [includeInactive])

    const ladyIds = ladiesResult.rows.map(item => item.id)

    if (!ladyIds.length) {
      return res.json({
        ok: true,
        count: 0,
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
