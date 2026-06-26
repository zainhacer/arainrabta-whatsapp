/**
 * ArainRabta WhatsApp Notification Service
 * ──────────────────────────────────────────
 * Zero-cost self-hosted WhatsApp gateway using whatsapp-web.js
 * 
 * Flow:
 *   Supabase DB trigger → whatsapp_queue table insert
 *   → Supabase Edge Function calls POST /send on this service
 *   → This service sends the WhatsApp message
 *
 * First run: scan QR code in terminal → session saved → auto-reconnects
 */

require('dotenv').config()

const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js')
const express   = require('express')
const qrcode    = require('qrcode-terminal')
const cron      = require('node-cron')
const { createClient } = require('@supabase/supabase-js')
// const ws = require('ws')

/* ─── Config ─────────────────────────────────────────────── */
const PORT          = process.env.PORT          || 3000
const SECRET        = process.env.WA_SERVICE_SECRET
const SESSION_NAME  = process.env.WA_SESSION_NAME || 'arainrabta'
const ADMIN_PHONE   = process.env.ADMIN_PHONE
const SUPABASE_URL  = process.env.SUPABASE_URL
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_ROLE_KEY

if (!SECRET)       { console.error('❌ WA_SERVICE_SECRET is required'); process.exit(1) }
if (!SUPABASE_URL) { console.error('❌ SUPABASE_URL is required');       process.exit(1) }

/* ─── Supabase client ─────────────────────────────────────── */
const ws = require('ws')

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  {
    realtime: {
      transport: (url, options) => new ws(url, options)
    }
  }
)

/* ─── State ───────────────────────────────────────────────── */
let waClient   = null
let isReady    = false
let qrString   = null
let stats      = { sent: 0, failed: 0, startTime: Date.now() }

/* ─── WhatsApp Client Setup ───────────────────────────────── */
function initWhatsApp() {
  console.log('🟡 WhatsApp client initializing...')

  waClient = new Client({
    authStrategy: new LocalAuth({ clientId: SESSION_NAME }),
    puppeteer: {
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--no-first-run',
        '--no-zygote',
        '--single-process',
      ],
    },
  })

  waClient.on('qr', (qr) => {
    qrString = qr
    isReady  = false
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('📱 WHATSAPP QR CODE — Terminal mein scan karein')
    console.log('   WhatsApp kholein → Linked Devices → Link a Device')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')
    qrcode.generate(qr, { small: true })
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('💡 Ya http://your-server:3000/qr par bhi dekh sakte hain')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')
  })

  waClient.on('ready', async () => {
    isReady  = true
    qrString = null
    const info = waClient.info
    console.log(`✅ WhatsApp connected! Number: ${info?.wid?.user}`)

    // Notify admin
    if (ADMIN_PHONE) {
      await safeSend(
        ADMIN_PHONE,
        `✅ ArainRabta WhatsApp Service online hai!\n🕐 ${new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' })}`
      )
    }

    // Process any queued messages
    await processQueue()
  })

  waClient.on('authenticated', () => {
    console.log('🔐 WhatsApp authenticated — session saved')
  })

  waClient.on('auth_failure', (msg) => {
    console.error('❌ WhatsApp auth failed:', msg)
    isReady = false
    // Retry after 30 seconds
    setTimeout(() => initWhatsApp(), 30_000)
  })

  waClient.on('disconnected', (reason) => {
    console.warn('⚠️  WhatsApp disconnected:', reason)
    isReady = false
    // Reconnect after 15 seconds
    setTimeout(() => {
      console.log('🔄 Reconnecting...')
      initWhatsApp()
    }, 15_000)
  })

  waClient.initialize().catch(err => {
    console.error('❌ WhatsApp init error:', err.message)
    setTimeout(() => initWhatsApp(), 30_000)
  })
}

/* ─── Safe send with retry ────────────────────────────────── */
async function safeSend(phone, message, retries = 2) {
  if (!isReady || !waClient) {
    console.warn(`⏸  Not ready, queuing message to ${phone}`)
    return { ok: false, reason: 'not_ready' }
  }

  // Normalize phone: remove +, spaces, dashes, ensure country code
  const cleaned = phone.replace(/[\s\-\(\)]/g, '').replace(/^\+/, '')
  // If starts with 0, replace with 92 (Pakistan)
  const normalized = cleaned.startsWith('0') ? '92' + cleaned.slice(1) : cleaned
  const chatId = `${normalized}@c.us`

  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      await waClient.sendMessage(chatId, message)
      stats.sent++
      console.log(`✅ Sent to ${normalized} (attempt ${attempt})`)
      return { ok: true, chatId }
    } catch (err) {
      console.warn(`⚠️  Attempt ${attempt} failed for ${normalized}: ${err.message}`)
      if (attempt <= retries) {
        await sleep(2000 * attempt)
      } else {
        stats.failed++
        return { ok: false, reason: err.message }
      }
    }
  }
}

/* ─── Process pending queue from DB ──────────────────────── */
async function processQueue() {
  if (!isReady) return

  const { data: pending } = await supabase
    .from('whatsapp_queue')
    .select('*')
    .eq('status', 'pending')
    .order('created_at', { ascending: true })
    .limit(20)

  if (!pending?.length) return

  console.log(`📤 Processing ${pending.length} queued messages...`)

  for (const item of pending) {
    // Mark as processing
    await supabase.from('whatsapp_queue')
      .update({ status: 'processing', updated_at: new Date().toISOString() })
      .eq('id', item.id)

    const result = await safeSend(item.phone, item.message)

    await supabase.from('whatsapp_queue').update({
      status:     result.ok ? 'sent' : 'failed',
      error:      result.ok ? null : result.reason,
      sent_at:    result.ok ? new Date().toISOString() : null,
      updated_at: new Date().toISOString(),
      attempts:   (item.attempts || 0) + 1,
    }).eq('id', item.id)

    // Small delay between messages to avoid rate limiting
    await sleep(1200)
  }
}

/* ─── Express HTTP Server ─────────────────────────────────── */
const app = express()
app.use(express.json())

// Auth middleware
const auth = (req, res, next) => {
  const key = req.headers['x-service-secret'] || req.query.secret
  if (key !== SECRET) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' })
  }
  next()
}

// Health check (public — for Render uptime monitoring)
app.get('/health', (req, res) => {
  res.json({
    ok:        true,
    status:    isReady ? 'connected' : 'disconnected',
    qr_needed: !!qrString,
    stats: {
      ...stats,
      uptime_minutes: Math.floor((Date.now() - stats.startTime) / 60000),
    },
  })
})

// QR code page (for scanning from browser when no terminal access)
app.get('/qr', (req, res) => {
  if (isReady) {
    return res.send(`
      <html><body style="font-family:sans-serif;text-align:center;padding:40px;background:#f0f7f4">
        <h1 style="color:#1B4332">✅ ArainRabta WhatsApp Service</h1>
        <p style="color:#059669;font-size:20px">Connected hai! Koi QR scan ki zaroorat nahi.</p>
      </body></html>
    `)
  }
  if (!qrString) {
    return res.send(`
      <html><body style="font-family:sans-serif;text-align:center;padding:40px">
        <h2>⏳ QR generate ho raha hai...</h2>
        <p>30 seconds mein refresh karein</p>
        <script>setTimeout(()=>location.reload(), 5000)</script>
      </body></html>
    `)
  }
  // Generate QR as SVG inline
  const qrLib = require('qrcode')
  qrLib.toDataURL(qrString, { width: 300 }, (err, url) => {
    res.send(`
      <html>
        <head>
          <title>ArainRabta WA QR</title>
          <meta name="viewport" content="width=device-width,initial-scale=1">
          <style>
            body { font-family:sans-serif; text-align:center; padding:20px; background:#f0f7f4; }
            img  { width:280px; border:8px solid white; border-radius:16px; box-shadow:0 4px 20px rgba(0,0,0,0.15); }
            h1   { color:#1B4332; }
            p    { color:#65676B; }
          </style>
        </head>
        <body>
          <h1>📱 ArainRabta WhatsApp QR Code</h1>
          <p>WhatsApp kholein → Linked Devices → Link a Device → Scan karein</p>
          <img src="${url}" alt="QR Code"/>
          <p style="margin-top:20px;color:#D97706">⚡ Yeh page 10 seconds mein auto-refresh hoga</p>
          <script>setTimeout(()=>location.reload(), 10000)</script>
        </body>
      </html>
    `)
  })
})

// Send a single message (called by Supabase Edge Function)
app.post('/send', auth, async (req, res) => {
  const { phone, message } = req.body
  if (!phone || !message) {
    return res.status(400).json({ ok: false, error: 'phone aur message zaroori hain' })
  }

  if (!isReady) {
    // Save to queue — will be sent when reconnected
    await supabase.from('whatsapp_queue').insert({
      phone, message, status: 'pending',
    })
    return res.json({ ok: true, queued: true, message: 'WhatsApp connected nahi — queue mein add kar diya' })
  }

  const result = await safeSend(phone, message)
  res.json(result)
})

// Send bulk (admin broadcast)
app.post('/send-bulk', auth, async (req, res) => {
  const { messages } = req.body // [{ phone, message }]
  if (!Array.isArray(messages) || !messages.length) {
    return res.status(400).json({ ok: false, error: 'messages array zaroori hai' })
  }

  res.json({ ok: true, total: messages.length, message: 'Bulk send shuru ho gaya' })

  // Process in background
  ;(async () => {
    for (const { phone, message } of messages) {
      if (isReady) {
        await safeSend(phone, message)
      } else {
        await supabase.from('whatsapp_queue').insert({ phone, message, status: 'pending' })
      }
      await sleep(1500) // rate limit protection
    }
    console.log(`✅ Bulk send complete — ${messages.length} messages`)
  })()
})

// Process queue manually (also called by cron)
app.post('/process-queue', auth, async (req, res) => {
  processQueue()
  res.json({ ok: true })
})

// Status
app.get('/status', auth, (req, res) => {
  res.json({
    ok:          true,
    is_ready:    isReady,
    qr_pending:  !!qrString,
    session:     SESSION_NAME,
    stats,
    wa_number:   isReady ? waClient?.info?.wid?.user : null,
  })
})

/* ─── Cron: process queue every 2 minutes ─────────────────── */
cron.schedule('*/2 * * * *', () => {
  if (isReady) processQueue()
})

/* ─── Cron: ping self every 14 min (Render free tier keepalive) ─ */
cron.schedule('*/14 * * * *', async () => {
  try {
    const axios = require('axios')
    await axios.get(`http://localhost:${PORT}/health`)
  } catch {}
})

/* ─── Start ───────────────────────────────────────────────── */
app.listen(PORT, () => {
  console.log(`\n🟢 ArainRabta WhatsApp Service started`)
  console.log(`   Port:    ${PORT}`)
  console.log(`   Health:  http://localhost:${PORT}/health`)
  console.log(`   QR Page: http://localhost:${PORT}/qr`)
  console.log(`   Session: ${SESSION_NAME}\n`)
  initWhatsApp()
})

/* ─── Utils ───────────────────────────────────────────────── */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

process.on('uncaughtException',  err  => console.error('Uncaught:', err.message))
process.on('unhandledRejection', err  => console.error('Unhandled:', err?.message || err))
