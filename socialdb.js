/*************************************************
 *  CASO: SocialDB (caso Redes Sociales)
 *  Autor: Rafael Buitrago 
 *************************************************/

use("socialdb");

/*1) CRUD BÁSICO (Insert, Find, Update, Delete)*/

// --- INSERT: usuario y post de prueba ---
const userTest = {
  username: "user_test",
  name: "Usuario Test",
  email: "user_test@example.com",
  created_at: new Date(),
  bio: "Perfil inicial"
};
db.users.insertOne(userTest);

// obtenemos el _id del usuario insertado
const userTestId = db.users.findOne({ username: "user_test" })._id;

// post de prueba asociado al usuario
const postTest = {
  user_id: userTestId,
  text: "Post de prueba con #mongodb y #bigdata",
  hashtags: ["#mongodb", "#bigdata"],
  created_at: new Date(),
  metrics: { likes: 0, comments: 0 }
};
db.posts.insertOne(postTest);

// --- SELECT: ver usuario y post recién insertados ---
db.users.find({ _id: userTestId });
db.posts.find({ user_id: userTestId }).sort({ created_at: -1 });

// --- UPDATE: cambiar bio del usuario y sumar likes al post ---
db.users.updateOne(
  { _id: userTestId },
  { $set: { bio: "Data enthusiast. #spark #kafka" } }
);

// tomar el post más reciente del usuario_test
const pTest = db.posts.find({ user_id: userTestId }).sort({ created_at: -1 }).limit(1).toArray()[0];
db.posts.updateOne(
  { _id: pTest._id },
  { $inc: { "metrics.likes": 5 } }
);

/*2) CONSULTAS CON FILTROS Y OPERADORES*/

// posts con hashtag #mongodb en la última semana
db.posts.find({
  hashtags: "#mongodb",
  created_at: { $gte: new Date(Date.now() - 7*24*3600*1000) }
}).sort({ created_at: -1 }).limit(10);

// usuarios cuyo username empiece por "user" y número >= 5 (regex)
db.users.find(
  { username: { $regex: /^user(10|[5-9])$/ } },
  { projection: { username: 1, email: 1 } }
);

// posts con >= 5 likes O con >= 3 comentarios
db.posts.find({
  $or: [
    { "metrics.likes":    { $gte: 5 } },
    { "metrics.comments": { $gte: 3 } }
  ]
}, { projection: { text: 1, metrics: 1 } }).limit(10);

// posts que contengan #spark o #kafka
db.posts.find({ hashtags: { $in: ["#spark", "#kafka"] } },
              { projection: { text: 1, hashtags: 1 } }).limit(10);

// documentos de posts SIN campo metrics (control de calidad de datos)
db.posts.find({ metrics: { $exists: false } }).limit(5);

// posts que tengan exactamente 3 hashtags
db.posts.find({ hashtags: { $size: 3 } },
              { projection: { hashtags: 1, text: 1 } }).limit(10);

// comentarios de un conjunto de posts (usando $in)
const somePosts = db.posts.find().limit(2).map(d => d._id);
db.comments.find({ post_id: { $in: somePosts } }).limit(5);


/*3)(contar/sumar/promediar)*/

// A) Top 10 hashtags por frecuencia de uso
db.posts.aggregate([
  { $unwind: "$hashtags" },
  { $group: { _id: "$hashtags", usos: { $sum: 1 } } },
  { $sort: { usos: -1 } },
  { $limit: 10 }
]);

// B) Posts con mayor engagement (likes + comments)
db.posts.aggregate([
  { $project: {
      text: 1,
      created_at: 1,
      engagement: { $add: ["$metrics.likes", "$metrics.comments"] }
  }},
  { $sort: { engagement: -1, created_at: -1 } },
  { $limit: 10 }
]);

// C) Usuarios más influyentes (likes totales recibidos)
//    + promedio de likes por post (join con users)
db.posts.aggregate([
  { $group: { _id: "$user_id", likes_totales: { $sum: "$metrics.likes" }, posts: { $sum: 1 } } },
  { $lookup: { from: "users", localField: "_id", foreignField: "_id", as: "u" } },
  { $unwind: "$u" },
  { $project: {
      _id: 0,
      username: "$u.username",
      posts: 1,
      likes_totales: 1,
      promedio_likes: {
        $cond: [{ $gt: ["$posts", 0] }, { $divide: ["$likes_totales", "$posts"] }, 0]
      }
  }},
  { $sort: { likes_totales: -1 } },
  { $limit: 10 }
]);

// D) Actividad por hora del día (conteo de posts)
db.posts.aggregate([
  { $addFields: { hour: { $hour: "$created_at" } } },
  { $group:   { _id: "$hour", posts: { $sum: 1 } } },
  { $sort:    { _id: 1 } }
]);
// --- DELETE: borrar post y luego el usuario de prueba ---
db.posts.deleteMany({ user_id: userTestId });
db.users.deleteOne({ _id: userTestId });
