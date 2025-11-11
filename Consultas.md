const dbx = db.getSiblingDB("socialdb");
//---CONSULTAS BASICAS---
// --- Inserción ---
dbx.users.insertOne({
  username: "usuario_demo",
  name: "Usuario Demo",
  email: "demo@example.com",
  createdAt: new Date(),
  status: "active",
  settings: { lang: "es", privacy: { profile: "public", dm: "all" } },
  counters: { followers: 0, following: 0, posts: 0 }
});

// --- Selección ---
dbx.posts.find({ visibility: "public" }).sort({ createdAt: -1 }).limit(5);

// --- Actualizacón ---
dbx.users.updateOne(
  { username: "usuario_demo" },
  { $set: { bio: "Actualicé mi bio" }, $inc: { "counters.posts": 1 } }
);

// --- Eliminación ---
dbx.users.deleteOne({ username: "usuario_demo" });

//---Consultas con filtros y operadores---

// === a) Filtros combinados con AND, OR, regex y comparadores ===
// Usuarios con perfil público, más de 10 seguidores y cuyo nombre empiece por A o C
dbx.users.find(
  {
    "settings.privacy.profile": "public",
    "counters.followers": { $gt: 10 },
    $or: [{ name: { $regex: /^A/i } }, { name: { $regex: /^C/i } }]
  },
  { _id: 0, username: 1, name: 1, "counters.followers": 1 }
);

// === b) Operadores IN y comparación de fechas ===
// Posts públicos o visibles a seguidores creados en los últimos 7 días
dbx.posts.find(
  {
    visibility: { $in: ["public", "followers"] },
    createdAt: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) }
  },
  { _id: 0, authorId: 1, visibility: 1, createdAt: 1 }
).sort({ createdAt: -1 }).limit(10);

// === c) Filtro por elementos en arrays ===
// Posts que contienen alguno de los hashtags “MongoDB” o “BigData”
dbx.posts.find(
  { hashtags: { $in: ["MongoDB", "BigData"] } },
  { _id: 0, authorId: 1, hashtags: 1, createdAt: 1 }
);

// === d) elemMatch ===
// Posts que mencionan a un usuario específico
const anyUser = dbx.users.findOne({}, { _id: 1 })._id;
dbx.posts.find({
  mentions: { $elemMatch: { userId: anyUser } }
});

// === e) exists / size ===
// Posts que tienen contenido multimedia (imágenes o videos)
dbx.posts.find({ media: { $exists: true, $ne: [] } }, { _id: 0, text: 1, media: 1 });

// Posts sin hashtags
dbx.posts.find({ hashtags: { $size: 0 } }, { _id: 0, text: 1, hashtags: 1 });

// === f) Búsqueda de texto parcial ===
// Comentarios que contienen la palabra “duda” (sin importar mayúsculas)
dbx.comments.find(
  { text: { $regex: /duda/i } },
  { _id: 0, text: 1, createdAt: 1, authorId: 1 }
).limit(10);

// === g) Combinación de operadores ===
// Usuarios activos con más de 5 publicaciones y menos de 50 seguidores
dbx.users.find(
  {
    status: "active",
    "counters.posts": { $gt: 5 },
    "counters.followers": { $lt: 50 }
  },
  { _id: 0, username: 1, "counters.posts": 1, "counters.followers": 1 }
);

// === h) Negaciones ===
// Usuarios cuyo perfil no sea público
dbx.users.find(
  { "settings.privacy.profile": { $ne: "public" } },
  { _id: 0, username: 1, "settings.privacy.profile": 1 }
);

// === i) Ordenamientos y límites ===
// Top 5 usuarios con más seguidores
dbx.users.find({}, { _id: 0, username: 1, "counters.followers": 1 })
  .sort({ "counters.followers": -1 })
  .limit(5);
//---Consultas de agregación para calcular estadísticas ---

// 1) Conteo de posts por usuario (TOP 10) con join a users
dbx.posts.aggregate([
  { $group: { _id: "$authorId", posts: { $sum: 1 } } },
  { $sort: { posts: -1 } },
  { $limit: 10 },
  { $lookup: { from: "users", localField: "_id", foreignField: "_id", as: "u" } },
  { $set: { username: { $first: "$u.username" } } },
  { $project: { _id: 0, username: 1, posts: 1 } }
]);

// 2) Promedio de likes y comentarios por post (2 decimales)
dbx.posts.aggregate([
  { $group: {
      _id: null,
      avgLikes: { $avg: "$metrics.likes" },
      avgComments: { $avg: "$metrics.comments" },
      totalPosts: { $sum: 1 }
  }},
  { $project: {
      _id: 0,
      totalPosts: 1,
      avgLikes: { $round: ["$avgLikes", 2] },
      avgComments: { $round: ["$avgComments", 2] }
  }}
]);

// 3) Top 5 hashtags
dbx.posts.aggregate([
  { $unwind: "$hashtags" },
  { $group: { _id: { $toLower: "$hashtags" }, posts: { $sum: 1 } } },
  { $sort: { posts: -1 } },
  { $limit: 5 }
]);

// 4) Posts por día (últimos 14 días)
dbx.posts.aggregate([
  { $match: { createdAt: { $gte: new Date(Date.now() - 14*24*60*60*1000) } } },
  { $group: { _id: { $dateTrunc: { date: "$createdAt", unit: "day" } }, n: { $sum: 1 } } },
  { $sort: { _id: 1 } }
]);

// 5) Total de likes recibidos por autor (TOP 5)
dbx.posts.aggregate([
  { $group: { _id: "$authorId", likesTotales: { $sum: "$metrics.likes" }, commentsTotales: { $sum: "$metrics.comments" } } },
  { $sort: { likesTotales: -1 } },
  { $limit: 5 },
  { $lookup: { from: "users", localField: "_id", foreignField: "_id", as: "u" } },
  { $set: { username: { $first: "$u.username" } } },
  { $project: { _id: 0, username: 1, likesTotales: 1, commentsTotales: 1 } }
]);

// 6) Seguidores por usuario (solo follows aceptados)
dbx.follows.aggregate([
  { $match: { status: "accepted" } },
  { $group: { _id: "$followeeId", seguidores: { $sum: 1 } } },
  { $sort: { seguidores: -1 } },
  { $limit: 5 },
  { $lookup: { from: "users", localField: "_id", foreignField: "_id", as: "u" } },
  { $set: { username: { $first: "$u.username" } } },
  { $project: { _id: 0, username: 1, seguidores: 1 } }
]);
