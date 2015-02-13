var murmurHash = require('../dist/murmurhash3_gc');
var cache      = {};
var _          = require('lodash');

class RowCache {
	constructor(query, params) {
		this.hashes = {
			query  : murmurHash(JSON.stringify(query)),
			params : murmurHash(JSON.stringify(params))
		};
	}

	add(key, obj) {
		if(!cache[this.hashes.query]) {
			cache[this.hashes.query] = {};
		}

		var queryCache = cache[this.hashes.query];

		if(!queryCache[key]) {
			queryCache[key] = {
				deps : {}
			};
		}

		var objCache = queryCache[key];

		objCache.deps[this.hashes.params] = true;
		objCache.obj = obj;

		return obj;
	}

	remove(key) {
		if(cache[this.hashes.query]) {
			var queryCache = cache[this.hashes.query];

			if(queryCache[key]) {
				var objCache = queryCache[key];

				delete objCache.deps[this.hashes.params];

				if(_.isEmpty(objCache.deps)) {
					delete queryCache[key];

					if(_.isEmpty(queryCache)) {
						delete cache[this.hashes.query];
					}
				}

				return objCache.obj;
			}
		}

		return null;
	}

	get(byRef, objKey) {
		var results = {};

		if(cache[this.hashes.query]) {
			var queryCache = cache[this.hashes.query];

			if(objKey) {
				if(queryCache[objKey]) {
					var objCache = queryCache[objKey];

					if(objCache.deps[this.hashes.params]) {
						return byRef ? objCache.obj : _.clone(objCache.obj);
					}
				}

				return null;
			}
			else {
				for(var key in queryCache) {
					var objCache = queryCache[key];

					if(queryCache[key].deps[this.hashes.params]) {
						results[key] = byRef ? objCache.obj : _.clone(objCache.obj);
					}
				}
			}
		}

		return results;
	}
}

module.exports = RowCache;
