# ECS Iconsiam ETL

A lightweight ETL (Extract, Transform, Load) script designed to run on AWS ECS instances. This script fetches data from an external API and syncs it to a PostgreSQL database using Prisma ORM.

## Features

- 🚀 **Lightweight**: Optimized for ECS deployment with minimal dependencies
- 🔄 **ETL Process**: Extract from external API, transform data, load to database
- 📊 **Batch Processing**: Processes records in configurable batches for better performance
- 🔍 **Smart Matching**: Intelligent floor and category matching with fallback strategies
- 📝 **Audit Logging**: Tracks sync operations with detailed metrics
- 🛡️ **Error Handling**: Comprehensive error handling and graceful shutdown
- 🏗️ **TypeScript**: Full TypeScript support with type safety

## Architecture

### Why Prisma instead of Payload?

For ECS deployment, we chose Prisma over Payload because:

1. **Smaller Footprint**: No CMS overhead, just database operations
2. **Faster Startup**: Direct database connection without CMS initialization
3. **Better Control**: More granular control over ETL operations
4. **Resource Efficient**: Lower memory and CPU usage
5. **Container Optimized**: Designed for serverless/containerized environments

## Prerequisites

- Node.js 18+
- PostgreSQL database
- External API access

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ecs-iconsiam
   ```

2. **Install dependencies**
   ```bash
   pnpm install
   ```

3. **Set up environment variables**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

4. **Generate Prisma client**
   ```bash
   pnpm db:generate
   ```

5. **Set up database**
   ```bash
   pnpm db:push
   ```

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Yes | - |
| `EXTERNAL_API_URL` | External API endpoint | Yes | - |
| `X_APIG_APPCODE` | API authentication key | No | - |
| `EXTERNAL_API_COOKIES` | API cookies (production) | No | - |
| `SYNC_BATCH_SIZE` | Records per batch | No | 10 |
| `NODE_ENV` | Environment | No | development |

### Database Schema

The script uses the following main tables:

- **shops**: Store shop information
- **dinings**: Store dining/restaurant information  
- **categories**: Store category mappings
- **floors**: Store floor information
- **sync_logs**: Audit trail for sync operations

## Usage

### Local Development

```bash
# Run in development mode
pnpm dev

# Run sync process
pnpm sync

# Build for production
pnpm build
```

### ECS Deployment

1. **Build Docker image**
   ```bash
   docker build -t ecs-iconsiam-etl .
   ```

2. **Push to ECR**
   ```bash
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
   docker tag ecs-iconsiam-etl:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/ecs-iconsiam-etl:latest
   docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/ecs-iconsiam-etl:latest
   ```

3. **ECS Task Definition**
   ```json
   {
     "family": "ecs-iconsiam-etl",
     "networkMode": "awsvpc",
     "requiresCompatibilities": ["FARGATE"],
     "cpu": "256",
     "memory": "512",
     "executionRoleArn": "arn:aws:iam::<account-id>:role/ecsTaskExecutionRole",
     "taskRoleArn": "arn:aws:iam::<account-id>:role/ecsTaskRole",
     "containerDefinitions": [
       {
         "name": "etl-container",
         "image": "<account-id>.dkr.ecr.us-east-1.amazonaws.com/ecs-iconsiam-etl:latest",
         "environment": [
           {
             "name": "DATABASE_URL",
             "value": "postgresql://..."
           },
           {
             "name": "EXTERNAL_API_URL",
             "value": "https://..."
           }
         ],
         "secrets": [
           {
             "name": "X_APIG_APPCODE",
             "valueFrom": "arn:aws:secretsmanager:..."
           }
         ],
         "logConfiguration": {
           "logDriver": "awslogs",
           "options": {
             "awslogs-group": "/ecs/ecs-iconsiam-etl",
             "awslogs-region": "us-east-1",
             "awslogs-stream-prefix": "ecs"
           }
         }
       }
     ]
   }
   ```

4. **EventBridge Rule**
   ```json
   {
     "Name": "ETLSyncRule",
     "ScheduleExpression": "rate(1 hour)",
     "Targets": [
       {
         "Id": "ETLSyncTarget",
         "Arn": "arn:aws:ecs:us-east-1:<account-id>:cluster/etl-cluster",
         "RoleArn": "arn:aws:iam::<account-id>:role/ecsEventsRole",
         "EcsParameters": {
           "TaskDefinitionArn": "arn:aws:ecs:us-east-1:<account-id>:task-definition/ecs-iconsiam-etl",
           "TaskCount": 1,
           "LaunchType": "FARGATE",
           "NetworkConfiguration": {
             "awsvpcConfiguration": {
               "Subnets": ["subnet-..."],
               "SecurityGroups": ["sg-..."],
               "AssignPublicIp": "DISABLED"
             }
           }
         }
       }
     ]
   }
   ```

## Data Flow

1. **Extract**: Fetch data from external API with pagination
2. **Transform**: 
   - Decode HTML entities and unicode escapes
   - Map floors and categories
   - Determine record type (shop vs dining)
   - Generate safe slugs
3. **Load**: 
   - Create or update records in database
   - Handle relationships (categories, floors)
   - Maintain audit trail

## Monitoring

### CloudWatch Logs

The script outputs structured logs for monitoring:

- Sync start/completion
- Batch processing progress
- Error details
- Performance metrics

### Sync Logs

Database table `sync_logs` tracks:

- Sync start/completion timestamps
- Records processed/created/updated
- Error details
- Performance metadata

## Error Handling

- **API Errors**: Retry logic with exponential backoff
- **Database Errors**: Transaction rollback and error logging
- **Validation Errors**: Detailed validation issue reporting
- **Graceful Shutdown**: Proper cleanup on SIGINT/SIGTERM

## Performance Optimization

- **Batch Processing**: Configurable batch sizes
- **Parallel Processing**: Concurrent record processing within batches
- **Connection Pooling**: Prisma handles database connection optimization
- **Memory Management**: Proper cleanup and garbage collection

## Security

- **Non-root User**: Docker container runs as non-root user
- **Secrets Management**: Use AWS Secrets Manager for sensitive data
- **Network Security**: VPC configuration with private subnets
- **IAM Roles**: Least privilege access

## Troubleshooting

### Common Issues

1. **Database Connection**: Check `DATABASE_URL` and network connectivity
2. **API Authentication**: Verify `X_APIG_APPCODE` and cookies
3. **Memory Issues**: Increase ECS task memory allocation
4. **Timeout Issues**: Adjust batch size or increase timeout settings

### Debug Mode

```bash
# Enable debug logging
DEBUG=prisma:* pnpm sync
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details # iconsiam-ecs-etl
