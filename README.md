# bulk_processing

export REDIS_PASSWORD=my_secure_token_123
export HOSPITAL_API_BASE_URL=https://hospital-directory.onrender.com

# First time or after changes
docker-compose up --build -d

# Check logs
docker-compose logs -f

# Stop when done
docker-compose down