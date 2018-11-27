const MongoClient   = require( 'mongodb' ).MongoClient;
const mysql         = require('mysql');
const Q             = require('q');
const async         = require("async");
const appConfig     = require('./appConfig');
const cron 			= require('node-cron');

// credentials
const mysqlServerUser = 'root';
const mysqlServerPassword = '';
const mysqlServerHost = '127.0.0.1';
const mysqlServerDBName = 'demo_db';

async function migrationScript() {
	console.log('Migrating data from demo_db MySQL database to its MongoDB' + '\n');
	let mongoDBConnection = await getMongoDBConnection();
	console.log('MongoDB connected successfully.' + '\n');

	result = await migrateUsers(mongoDBConnection);
	if(!result) {
		console.log('ERROR: user => user');
		process.exit(1);
	}
	console.log('user table data migrated into user collection' + '\n');
	console.log('=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*');
	

	result = await migrateUserProfile(mongoDBConnection);
	if(!result) {
		console.log('ERROR: user_profile => userProfile');
		process.exit(1);
	}
	console.log('user_profile table data migrated into userProfile collection' + '\n');
	console.log('=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*');

	result = await migrateGalleryImages(mongoDBConnection);
	if(!result) {
		console.log('ERROR: user => user');
		process.exit(1);
	}
	console.log('gallery_image table data migrated into GalleryImage collection' + '\n');
	console.log('=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*');
	

	result = await migrateGallery(mongoDBConnection);
	if(!result) {
		console.log('ERROR: user_profile => userProfile');
		process.exit(1);
	}
	console.log('gallery table data migrated into Gallery collection' + '\n');
	console.log('=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*');

	process.exit(0);
}

/* USERS */
async function migrateUsers(mongoDBConnection) {
	const mySQLTableName = 'user';
	const mongoDBCollectionName = 'user';
	const sql = 'SELECT * FROM `' + mySQLTableName + '`;';
	let users = await mySQLQuery(sql); // get main_categories from mysql databse
	console.log('Found ' + users.length + 'users Data in mySQL databse' + '\n');
	let usersData = await mapUsers(users);
	console.log('Mapped', usersData.length, 'users Data for mongoDB' + '\n');
	let result = await importDataInMongoCollection(mongoDBConnection, mongoDBCollectionName, usersData);
	return result;
}

function mapUsers(users) {
	let deferred = Q.defer();
	let usersData = [];
	async.forEachLimit(users, 1, function(imageData, callback) {
		let params = {
			sqlId: imageData.id,
			mapId: imageData.id,
			name: imageData.name,
			age: imageData.age,
			city: imageData.city,
		};
		usersData.push(params);
		process.nextTick(callback);
	}, function(err) {
		if (err) return deferred.reject(err);
		return deferred.resolve(usersData);
	});
	return deferred.promise;
}

/* USER PROFILE */
async function migrateUserProfile(mongoDBConnection) {
	const mySQLTableName = 'user_profile';
	const mongoDBCollectionName = 'userProfile';
	const sql = 'SELECT * FROM `' + mySQLTableName + '`;';
	let getUserProfile = await mySQLQuery(sql); // getAdminUsers from mysql databse
	console.log('Found' + getUserProfile.length + 'user profile in mySQL databse' + '\n');
	let mapedProfileData = await mapUserProfile(getUserProfile);
	console.log('Mapped', mapedProfileData.length, 'user profile for mongoDB' + '\n');
	let profile = await mapIdsOfUsers(mapedProfileData, mongoDBConnection);
	console.log('Mapped Ids', profile.length, 'user profile for mongoDB' + '\n');
	let result = await importDataInMongoCollection(mongoDBConnection, mongoDBCollectionName, profile);
	return result;
}

function mapUserProfile(userProfile) {
	let deferred = Q.defer();
	let profileDetailsData = [];
	async.forEachLimit(userProfile, 1, function(profile, callback) {
		let params = {
			'profileDetail' : profile.profile_detail,
			'profilePic' : profile.profile_pic,
			'profileId' : [],
			'Id' : profile.profile_id,
			'sqlId': profile.id

		}
		profileDetailsData.push(params);
		process.nextTick(callback);
		}, function(err) {
		if (err) return deferred.reject(err);
		return deferred.resolve(profileDetailsData);
	});
	return deferred.promise;
}

function mapIdsOfUsers(mapedProfile, mongoDBConnection) {
	let deferred = Q.defer();
	const userProfileCollection = mongoDBConnection.collection("user");
	let mapedProfileData = [];
	userProfileCollection.find({}).toArray(function(error, userProfiles) {
		if(error) console.log(error);
		if(userProfiles && userProfiles.length > 0) {
			mapedProfile.forEach(function(profile, index) {
				userProfiles.forEach(function(user, index){
					if(user.sqlId){
						if (user.mapId == profile.Id) {
							profile.profileId.push(user._id);
						}
					}
				});
				mapedProfileData.push(profile);
			});
		}
		return deferred.resolve(mapedProfileData);
	})
	return deferred.promise;
}

/* GAALLERY IMAGE*/
async function migrateGalleryImages(mongoDBConnection) {
	const mySQLTableName = 'gallery_image';
	const mongoDBCollectionName = 'GalleryImage';
	const sql = 'SELECT * FROM `' + mySQLTableName + '`;';
	let mainGalleryImage = await mySQLQuery(sql); // get main_categories from mysql databse
	console.log('Found ' + mainGalleryImage.length + 'Images Data in mySQL databse' + '\n');
	let galleryImage = await mapGalleryImage(mainGalleryImage);
	console.log('Mapped', galleryImage.length, 'Images Data for mongoDB' + '\n');
	let result = await importDataInMongoCollection(mongoDBConnection, mongoDBCollectionName, galleryImage);
	return result;
}

function mapGalleryImage(mainGalleryImage) {
	let deferred = Q.defer();
	let galleryImagesData = [];
	async.forEachLimit(mainGalleryImage, 1, function(imageData, callback) {
		let params = {
			sqlId: imageData.id,
			mapId: imageData.gallery_id,
			image: imageData.image
		};
		galleryImagesData.push(params);
		process.nextTick(callback);
	}, function(err) {
		if (err) return deferred.reject(err);
		return deferred.resolve(galleryImagesData);
	});
	return deferred.promise;
}

/* GAALLERY*/
async function migrateGallery(mongoDBConnection) {
	const mySQLTableName = 'gallery';
	const mongoDBCollectionName = 'Gallery';
	const sql = 'SELECT * FROM `' + mySQLTableName + '`;';
	let getSqlGallery = await mySQLQuery(sql); // getAdminUsers from mysql databse
	console.log('Found' + getSqlGallery.length + 'gallery in mySQL databse' + '\n');
	let mapedGallery = await mapGallery(getSqlGallery);
	console.log('Mapped', mapedGallery.length, 'gallery for mongoDB' + '\n');
	let Gallery = await mapImagesOfGallery(mapedGallery, mongoDBConnection);
	console.log('Mapped Images', Gallery.length, 'gallery for mongoDB' + '\n');
	let result = await importDataInMongoCollection(mongoDBConnection, mongoDBCollectionName, Gallery);
	return result;
}

function mapGallery(getGallery) {
	let deferred = Q.defer();
	let Gallerys = [];
	async.forEachLimit(getGallery, 1, function(Gallery, callback) {
		let GalleryParams = {
			'name' : Gallery.title,
			'description' : Gallery.title,
			'GalleryImages' : [],
			'sqlId' : Gallery.id,
			'Id' : Gallery.id,
		}
		Gallerys.push(GalleryParams);
		process.nextTick(callback);
		}, function(err) {
		if (err) return deferred.reject(err);
		return deferred.resolve(Gallerys);
	});
	return deferred.promise;
}

function mapImagesOfGallery(mapedGallery, mongoDBConnection) {
	let deferred = Q.defer();
	const GalleryImageCollection = mongoDBConnection.collection("GalleryImage");
	let mapedGalleryData = [];
	GalleryImageCollection.find({}).toArray(function(error, GalleryImages) {
		if(error) console.log(error);
		if(GalleryImages && GalleryImages.length > 0) {
			mapedGallery.forEach(function(Gallery, index) {
				GalleryImages.forEach(function(image, index){
					if(image.sqlId){
						if (image.mapId == Gallery.Id) {
							Gallery.GalleryImages.push(image._id);
						}
					}
				});
				mapedGalleryData.push(Gallery);
			});
		}
		return deferred.resolve(mapedGalleryData);
	})
	return deferred.promise;
}


/* Database connection and configuration functions */
function getMongoDBConnection() {
	let deferred = Q.defer();
	MongoClient.connect(appConfig.MONGO_DB_URL,{useNewUrlParser: true}, function( err, db ) {
		if (err) {
			console.log('Unable to connect to MongoDB Server. Error: ' + err);
			return deferred.reject(err);
		}
		console.log('connected to database :: ' + appConfig.MONGO_DB_URL);
		return deferred.resolve(db);
	});
	return deferred.promise;
}
/* Database connection and configuration functions */

/* Database fetch and update functions */ 
function importDataInMongoCollection(mongoDBConnection, collectionName, data) {
	let deferred = Q.defer();
	const connection = mongoDBConnection.collection(collectionName);
	let counter = 0;
	async.forEachLimit(data, 1, function(result, callback) {
		let findQuery;
		findQuery = { sqlId: result.sqlId };
		connection.update(findQuery, { $set: result }, { safe: true, upsert: true}, function(err, updatedDoc) {
			if (err) return deferred.reject(err);
			counter++;
			process.nextTick(callback);
		});
	}, function (err) {
		if (err) return deferred.reject(err);
		console.log('MongoDB no. of documents inserted/updated', counter , '\n');
		return deferred.resolve(true);
	});
	return deferred.promise;
}

function mySQLQuery(sql) {
	let deferred = Q.defer();
	let connection = mysql.createConnection({
		host: mysqlServerHost,
		user: mysqlServerUser,
		password: mysqlServerPassword,
		port: 3306,
		database: mysqlServerDBName
	});
	connection.connect();
	connection.query(sql, function (err, results, fields) {
		connection.end();
		if (err) {
			return deferred.reject(err);
		}
		return deferred.resolve(results);
	});
	return deferred.promise;
}
/* Database fetch and update functions */

cron.schedule('0 * * * *', () => {
  	console.log('running a migrationScript every hour');
	migrationScript();
});