'use strict';

module.exports = DocumentDatabase;

function DocumentDatabase(documentDbClient) {
    this.client = documentDbClient;
}

DocumentDatabase.prototype = {
    getOrCreateDatabase: function getOrCreateDatabase(databaseName, callback) {
        var self = this;

        var querySpec = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [{
                name: '@id',
                value: databaseName
            }]
        };

        self.client.queryDatabases(querySpec).toArray(function (err, results) {
            if (err) {
                callback(err);

            } else {
                if (results.length === 0) {
                    var databaseSpec = {
                        id: databaseName
                    };

                    self.client.createDatabase(databaseSpec, function (err, database) {
                        callback(null, database);
                    });

                } else {
                    callback(null, results[0]);
                }
            }
        });
    },

    getOrCreateCollection: function getOrCreateCollection(databaseLink, collectionName, callback) {
        var self = this;

        var querySpec = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [{
                name: '@id',
                value: collectionName
            }]
        };

        self.client.queryCollections(databaseLink, querySpec).toArray(function (err, collections) {
            if (err) {
                callback(err);

            } else {
                if (collections.length === 0) {
                    var collectionSpec = {
                        id: collectionName
                    };

                    var requestOptions = {
                        offerType: 'S1'
                    };

                    self.client.createCollection(databaseLink, collectionSpec, requestOptions, function (err, collection) {
                        callback(null, collection);
                    });

                } else {
                    callback(null, collections[0]);
                }
            }
        });
    }
};
