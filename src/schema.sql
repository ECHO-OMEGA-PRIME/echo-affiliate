-- Echo Affiliate v1.0.0 Schema

CREATE TABLE IF NOT EXISTS programs (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  slug TEXT UNIQUE,
  description TEXT,
  commission_type TEXT DEFAULT 'percentage' CHECK(commission_type IN ('percentage','flat','tiered','recurring')),
  commission_value REAL DEFAULT 10,
  commission_tiers TEXT DEFAULT '[]',
  recurring_months INTEGER DEFAULT 0,
  cookie_days INTEGER DEFAULT 30,
  min_payout REAL DEFAULT 50,
  payout_frequency TEXT DEFAULT 'monthly' CHECK(payout_frequency IN ('weekly','biweekly','monthly','manual')),
  currency TEXT DEFAULT 'USD',
  auto_approve INTEGER DEFAULT 1,
  terms TEXT,
  is_active INTEGER DEFAULT 1,
  total_affiliates INTEGER DEFAULT 0,
  total_revenue REAL DEFAULT 0,
  total_commissions REAL DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS affiliates (
  id TEXT PRIMARY KEY,
  program_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  company TEXT,
  website TEXT,
  ref_code TEXT UNIQUE NOT NULL,
  status TEXT DEFAULT 'pending' CHECK(status IN ('pending','approved','active','suspended','rejected')),
  tier INTEGER DEFAULT 1,
  parent_id TEXT,
  custom_commission_type TEXT,
  custom_commission_value REAL,
  total_clicks INTEGER DEFAULT 0,
  total_referrals INTEGER DEFAULT 0,
  total_conversions INTEGER DEFAULT 0,
  total_revenue REAL DEFAULT 0,
  total_earned REAL DEFAULT 0,
  total_paid REAL DEFAULT 0,
  balance REAL DEFAULT 0,
  payment_method TEXT CHECK(payment_method IN ('paypal','bank','stripe','check')),
  payment_details TEXT DEFAULT '{}',
  last_active_at TEXT,
  approved_at TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (program_id) REFERENCES programs(id)
);

CREATE TABLE IF NOT EXISTS links (
  id TEXT PRIMARY KEY,
  affiliate_id TEXT NOT NULL,
  program_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  destination_url TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL,
  utm_source TEXT,
  utm_medium TEXT,
  utm_campaign TEXT,
  total_clicks INTEGER DEFAULT 0,
  unique_clicks INTEGER DEFAULT 0,
  total_conversions INTEGER DEFAULT 0,
  conversion_value REAL DEFAULT 0,
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (affiliate_id) REFERENCES affiliates(id),
  FOREIGN KEY (program_id) REFERENCES programs(id)
);

CREATE TABLE IF NOT EXISTS clicks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  link_id TEXT NOT NULL,
  affiliate_id TEXT NOT NULL,
  program_id TEXT NOT NULL,
  visitor_ip_hash TEXT,
  visitor_ua TEXT,
  referrer TEXT,
  country TEXT,
  device TEXT,
  is_unique INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (link_id) REFERENCES links(id)
);

CREATE TABLE IF NOT EXISTS conversions (
  id TEXT PRIMARY KEY,
  affiliate_id TEXT NOT NULL,
  program_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  link_id TEXT,
  order_id TEXT,
  customer_email TEXT,
  revenue REAL NOT NULL DEFAULT 0,
  commission REAL NOT NULL DEFAULT 0,
  commission_type TEXT,
  status TEXT DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected','reversed')),
  is_recurring INTEGER DEFAULT 0,
  recurring_month INTEGER DEFAULT 1,
  parent_conversion_id TEXT,
  sub_affiliate_id TEXT,
  fraud_score REAL DEFAULT 0,
  fraud_flags TEXT DEFAULT '[]',
  metadata TEXT DEFAULT '{}',
  approved_at TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (affiliate_id) REFERENCES affiliates(id),
  FOREIGN KEY (program_id) REFERENCES programs(id)
);

CREATE TABLE IF NOT EXISTS payouts (
  id TEXT PRIMARY KEY,
  affiliate_id TEXT NOT NULL,
  program_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  amount REAL NOT NULL,
  currency TEXT DEFAULT 'USD',
  method TEXT,
  reference TEXT,
  status TEXT DEFAULT 'pending' CHECK(status IN ('pending','processing','completed','failed','cancelled')),
  period_start TEXT,
  period_end TEXT,
  conversions_count INTEGER DEFAULT 0,
  processed_at TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (affiliate_id) REFERENCES affiliates(id)
);

CREATE TABLE IF NOT EXISTS creatives (
  id TEXT PRIMARY KEY,
  program_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  type TEXT DEFAULT 'banner' CHECK(type IN ('banner','text','email','video','social')),
  content TEXT DEFAULT '{}',
  dimensions TEXT,
  url TEXT,
  total_uses INTEGER DEFAULT 0,
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (program_id) REFERENCES programs(id)
);

CREATE TABLE IF NOT EXISTS analytics_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant_id TEXT NOT NULL,
  program_id TEXT,
  date TEXT NOT NULL,
  clicks INTEGER DEFAULT 0,
  unique_clicks INTEGER DEFAULT 0,
  conversions INTEGER DEFAULT 0,
  revenue REAL DEFAULT 0,
  commissions REAL DEFAULT 0,
  signups INTEGER DEFAULT 0,
  payouts REAL DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(tenant_id, program_id, date)
);

CREATE TABLE IF NOT EXISTS fraud_rules (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  type TEXT DEFAULT 'auto' CHECK(type IN ('auto','manual')),
  rule TEXT DEFAULT '{}',
  action TEXT DEFAULT 'flag' CHECK(action IN ('flag','reject','suspend')),
  is_active INTEGER DEFAULT 1,
  triggers_count INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS activity_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant_id TEXT NOT NULL,
  program_id TEXT,
  affiliate_id TEXT,
  action TEXT NOT NULL,
  details TEXT DEFAULT '{}',
  actor TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_affiliates_program ON affiliates(program_id, status);
CREATE INDEX IF NOT EXISTS idx_affiliates_ref ON affiliates(ref_code);
CREATE INDEX IF NOT EXISTS idx_affiliates_parent ON affiliates(parent_id);
CREATE INDEX IF NOT EXISTS idx_links_affiliate ON links(affiliate_id);
CREATE INDEX IF NOT EXISTS idx_links_slug ON links(slug);
CREATE INDEX IF NOT EXISTS idx_clicks_link ON clicks(link_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_clicks_affiliate ON clicks(affiliate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversions_affiliate ON conversions(affiliate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversions_program ON conversions(program_id, status);
CREATE INDEX IF NOT EXISTS idx_conversions_order ON conversions(order_id);
CREATE INDEX IF NOT EXISTS idx_payouts_affiliate ON payouts(affiliate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_status ON payouts(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_analytics_tenant ON analytics_daily(tenant_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_activity_tenant ON activity_log(tenant_id, created_at DESC);
