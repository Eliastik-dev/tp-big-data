# etape 1 : build du front react
FROM node:20-slim AS build
WORKDIR /app/client
COPY client/package*.json ./
RUN npm install
COPY client/ ./
RUN npm run build

# etape 2 : serveur node qui sert le front + l'api
FROM node:20-slim
WORKDIR /app/serveur
COPY serveur/package*.json ./
RUN npm install --omit=dev
COPY serveur/ ./
COPY --from=build /app/serveur/public ./public
EXPOSE 3000
CMD ["node", "index.js"]
