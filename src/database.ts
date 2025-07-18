import * as dotenv from 'dotenv'

// Load environment variables
dotenv.config()

// Construct DATABASE_URL from individual components
function constructDatabaseUrl(): string {
  // If DATABASE_URL is already a complete connection string (like Neon), use it as is
  if (process.env.DATABASE_URL && process.env.DATABASE_URL.includes('://')) {
    return process.env.DATABASE_URL
  }

  // Otherwise, construct from individual components (for AWS RDS)
  const host = process.env.DATABASE_URL || 'localhost'
  const port = process.env.DATABASE_PORT || '5432'
  const database = process.env.DATABASE_NAME || 'iconsiam_web_dev'
  const user = process.env.DATABASE_USER || 'iconsiamWebDB'
  const password = process.env.DB_PASS || ''
  const sslMode = process.env.DATABASE_SSL_MODE === 'true' ? 'require' : 'prefer'

  return `postgresql://${user}:${password}@${host}:${port}/${database}?sslmode=${sslMode}`
}

// Set the DATABASE_URL environment variable
process.env.DATABASE_URL = constructDatabaseUrl()

export { constructDatabaseUrl } 