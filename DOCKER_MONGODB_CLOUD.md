MongoDB Cloud Deployment With Docker

1) Provision a cloud VM
- Use Ubuntu 22.04 or Debian 12.
- Install Docker and Docker Compose plugin.

2) Upload this project to the VM and enter the directory.

3) Prepare secrets
- Copy .mongo.env.example to .mongo.env.
- Set strong passwords in .mongo.env.

4) Start services
- docker compose --env-file .mongo.env -f docker-compose.mongodb.yml up -d
- docker compose --env-file .mongo.env -f docker-compose.mongodb.yml ps

5) Verify MongoDB inside VM
- docker exec -it soen363-mongo mongosh -u admin -p 'YOUR_MONGO_ROOT_PASSWORD' --authenticationDatabase admin

6) Access from your laptop through SSH tunnel (recommended)
- ssh -N -L 27017:127.0.0.1:27017 user@your-vm-ip
- Then connect locally using:
  mongodb://admin:YOUR_PASSWORD@localhost:27017/?authSource=admin

7) Optional: Mongo Express web UI through SSH tunnel
- ssh -N -L 8081:127.0.0.1:8081 user@your-vm-ip
- Open http://localhost:8081

8) Use this URI in your migration script
- MONGODB_URI=mongodb://admin:YOUR_PASSWORD@localhost:27017/?authSource=admin
- MONGODB_DATABASE=hospital

9) Stop services
- docker compose -f docker-compose.mongodb.yml down

Security notes
- Do not expose 27017 publicly on the internet.
- Keep VM firewall closed for 27017 and 8081.
- Use SSH tunnels or a private network/VPN.
