'use strict';

var DocumentDatabase = require('./lib/documentDatabase');
var DocumentClient = require('documentdb').DocumentClient;
var db;

module.exports = DocumentRepository;

function DocumentRepository(dbConfig, databaseName, collectionName) {
    var docDbClient = new DocumentClient(dbConfig.documentDbUrl, {
        masterKey: dbConfig.documentDbKey
    });

    db = new DocumentDatabase(docDbClient);
    this.client = docDbClient;
    this.databaseName = databaseName;
    this.collectionName = collectionName;

    this.database = null;
    this.collection = null;
}

DocumentRepository.prototype = {
    init: function (callback) {
        var self = this;

        db.getOrCreateDatabase(self.databaseName, function (err, database) {
            if (err) {
                callback(err);
            } else {
                db.getOrCreateCollection(database._self, self.collectionName, function (err, coll) {
                    if (err) {
                        callback(err);

                    } else {
                        self.collection = coll;
                        callback();
                    }
                });
            }
        });
    },
    getById: function getById(id, callback) {
        var self = this;
        var query = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [{
                name: '@id',
                value: id
            }]
        };

        this.client.queryDocuments(self.collection._self, query).toArray(function (err, results) {
            if (err) {
                callback(err);
            } else {
                callback(null, results[0]);
            }
        });
    },
    getByMerchant: function getById(merchantId, callback) {
        var self = this;
        var query = {
            query: 'SELECT * FROM root r WHERE r.merchantId=@merchantId',
            parameters: [{
                name: '@merchantId',
                value: merchantId
            }]
        };

        this.client.queryDocuments(self.collection._self, query).toArray(function (err, results) {
            if (err) {
                callback(err);
            } else {
                callback(null, results);
            }
        });
    },
    findAll: function findAll(callback) {
        var self = this;
        var query = {
            query: 'SELECT * FROM root'
        };

        this.client.queryDocuments(self.collection._self, query).toArray(function (err, results) {
            if (err) {
                callback(err);
            } else {
                callback(null, results);
            }
        });
    },

    add: function add(item, callback) {
        var self = this;

        this.client.createDocument(self.collection._self, item, function (err, doc) {
            callback(err, doc);
        });
    },

    update: function update(item, callback) {
        var self = this;

        self.getById(item.id, function (err, doc) {
            if (err) {
                callback(err);
            } else {
                self.client.replaceDocument(doc._self, item, function (err, replacedDoc) {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, replacedDoc);
                    }
                });
            }
        });
    },

    addOrUpdate: function addOrUpdate(item, callback) {
        var self = this;

        self.getById(item.id, function (err, doc) {
            if (err) {
                callback(err);
            } else {
                if (!doc) {
                    self.client.createDocument(self.collection._self, item, callback);
                } else {
                    // TODO: check for concurrency problems
                    self.client.replaceDocument(doc._self, item, callback);
                }
            }
        });
    }
};
