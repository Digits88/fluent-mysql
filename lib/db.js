/**
 * An unassuming fluent interface to node_mysql
 */
var mysql = require('mysql');

/*
 Builds query segments. Does not know the statement context,
 hence cannot build complete statements.

 @param conn {mysql.conn} An instance retrieved from `mysql.createConnection`.
 */
var QueryBuilder = function(pool) {

    var builder = this;
    this.pool = pool;
    this.nestTables = false;
    this.values = {
        object: null,
        exclude: []
    };
    this.conditions = [];
    this.joinConditions = [];
    this.groupFields = null;
    this.orderFields = null;
    this.orderAsc = true;
    this.limitRows = 0;

    /*
     Constructs a set statement given an object and exclusions.

     @param obj {Object} Object to use in the set statement.
     @param exclude {Array} Array of field names to exclude.
     */
    this.setFromObject = function(obj, exclude) {

        var setSegment = [],
            keys = Object.keys(obj);

        for(var i = 0,length = keys.length; i < length; i++) {
            var ignore = false,
                name = keys[i];
            if (exclude && exclude.length) {
                ignore = (exclude.indexOf(name) >= 0);
            }
            if (!ignore) {
                setSegment.push(name + " = " + this.escape(obj[name]));
            }
        }
        return  setSegment.length > 0 ?
            " SET " + setSegment.join(", ") : " () VALUES ()";
    };

    /*
     Constructs a where statement an array of conditions. Currently appends
     the conditions with an `AND` conjunction.

     @param conditions {Array} Array of `condition` objects.
     */
    this.whereFromConditions = function(conditions) {
        var length = conditions.length;
        if (!length) return "";

        var conditionString = " WHERE ";
        for (var i=0; i<length; i++) {
            conditionString += conditions[i].toString();
            if (i < length-1) {
                conditionString += " AND ";
            }
        }
        return conditionString;
    };

    this.joinFromConditions = function (conditions) {
        return conditions.reduce(function (prev, curr) {
            return prev + " " + curr.toString();
        }, "");
    };

    this.statementFromFields = function (fields, groupBy) {
        if (!fields || fields.length == 0) return "";

        return (groupBy ? " GROUP BY " : " ORDER BY ") + fields.reduce(function (prev, curr) {
            return prev ? prev + ", " + curr : curr;
        }, null);
    };

    /*
     Delagates the db client for escaping the value.
     */
    this.escape = function(val) {
        return mysql.escape(val);
    };

    /*
     The condition object.

     @param field {String} The name of the field.
     @param value {any} The value.
     @param operator {String} The operator for the condition. Defaults to `=`.
     */
    this.condition = function(field, value, operator, escapeValue) {
        this.field = field;
        this.operator = operator || "=";
        this.value = value;
        var escape = escapeValue == undefined || escapeValue;

        this.toString = function() {
            return this.field + " " +
                this.operator + " " +
                (escape ? builder.escape(this.value) : this.value);
        };
    };

    this.leftJoinCondition = function (table, field1, field2) {
        this.table = table;
        this.field1 = field1;
        this.field2 = field2;

        this.toString = function() {
            builder.nestTables = true;
            return "LEFT JOIN " + this.table + " ON " + this.field1 + " = " + this.field2;
        };
    };

    this.innerJoinCondition = function (table, field1, field2) {
        this.table = table;
        this.field1 = field1;
        this.field2 = field2;

        this.toString = function() {
            builder.nestTables = true;
            return "INNER JOIN " + this.table + " ON " + this.field1 + " = " + this.field2;
        };
    };

    /*
     The join condition object (A.field1 = B.field2).

     @param field1 {String} First field in the condition.
     @param field2 {String} Second fied in the condition.
     */
    this.joinCondition = function(field1, field2) {
        this.toString = function() {
            builder.nestTables = true;
            return new builder.condition(field1, field2, "=", false).toString();
        }
    };

    /*
     Executes the given query.

     @param query {String} A well formed sql query.
     @param cb {Function} Callback - for INSERTS gets err and the created entity as params, for others see node-mysql for signature.
     */
    this.exec = function(query, cb) {
        var self = this;
        // console.log(query);

        builder.pool.getConnection(function (err, conn) {
            if (err) return cb(err);

            if (query.indexOf("INSERT INTO") >= 0) {
                conn.query(query,
                    function (err, info) {
                        var obj = self.values && self.values.object;
                        if (!err) {
                            if (obj && info && info.insertId) {
                                obj.id = info.insertId;
                            }
                        }
                        conn.release();
                        cb(err, obj || info);
                    }
                );
            } else {
                conn.query(builder.nestTables ? { sql: query, nestTables: true } : query, function (err, info) {
                    conn.release();
                    cb(err, info);
                });
            }
        });
    };

    /*
     Accepts the parameters for creating set statement.

     @param obj {Object} The object for the set statement.
     @param exclude {Array} The array of fields to exclude.
     */
    this.set = function(obj, exclude) {
        this.values.object = obj;
        this.values.exclude = exclude;
    };

    /*
     Accepts a where condition. This function must be called
     multiple times for multiple conditions.

     @param field {String} The name of the field.
     @param value {any} The value.
     @param operator {String} The operator for the condition. Defaults to `=`.
     */
    this.where = function(field, value, operator) {
        this.conditions.push(new this.condition(field, value, operator, !(operator && operator.toUpperCase() === "IN")));
    };

    /*
     Adds a join condition (A.field1 = B.field2).

     @param field1 {String}
     @param field2 {String}
     */
    this.join = function(field1, field2) {
        this.conditions.push(new this.joinCondition(field1, field2));
    };

    /*
     Adds a left outer join condition for the specified table (A.field1 = B.field2).

     @param table {String}
     @param field1 {String}
     @param field2 {String}
     */
    this.leftJoin = function(table, field1, field2) {
        this.joinConditions.push(new this.leftJoinCondition(table, field1, field2));
    };

    /*
     Adds an inner join condition for the specified table (A.field1 = B.field2).

     @param table {String}
     @param field1 {String}
     @param field2 {String}
     */
    this.innerJoin = function(table, field1, field2) {
        this.joinConditions.push(new this.innerJoinCondition(table, field1, field2));
    };

    /*
     Adds fields to be used in "GROUP BY" clause.

     @param fields {Array}
     */
    this.groupBy = function(fields) {
        this.groupFields = fields;
    };


    this.orderBy = function(fields, asc) {
        this.orderFields = fields;
        this.orderAsc = asc;
    };

    /*
     Adds a limit to the returned number of rows.

     @param rows {Number}
     */
    this.limit = function(rows) {
        this.limitRows = rows;
    };

    /*
     @property setSegment.

     Returns set statement from a previously called set method.
     */
    this.__defineGetter__('setSegment', function() {
        return this.setFromObject(this.values.object, this.values.exclude);
    });

    /*
     @property whereSegment.

     Returns where statement from a previously called set method.
     */
    this.__defineGetter__('whereSegment', function() {
        return this.whereFromConditions(this.conditions);
    });

    this.__defineGetter__('joinSegment', function() {
        return this.joinFromConditions(this.joinConditions);
    });

    this.__defineGetter__('groupBySegment', function() {
        return this.statementFromFields(this.groupFields, true);
    });

    this.__defineGetter__('orderBySegment', function() {
        var statement = this.statementFromFields(this.orderFields, false);
        if (statement == "") return "";
        return statement + (this.orderAsc ? " ASC" : " DESC");
    });

    this.__defineGetter__('limitSegment', function() {
        return this.limitRows > 0 ? " LIMIT " + this.limitRows : "";
    })
};


/*
 Chainable query builder interface for building
 Insert statements.
 */
var InsertBuilder = function(pool, tableName) {

    this.builder = new QueryBuilder(pool);
    this.tableName = tableName;

    this.set = function(obj, exclude) {
        this.builder.set(obj, exclude);
        return this;
    };

    this.sql = function() {
        return  "INSERT INTO " + this.tableName + this.builder.setSegment;
    };

    this.exec = function(cb) {
        this.builder.exec(this.sql(), cb);
    };
};

/*
 Chainable query builder interface for building
 Insert statements that insert multiple rows.
 */
var MultiInsertBuilder = function(pool, tableName) {

    this.builder = new QueryBuilder(pool);
    this.tableName = tableName;

    this.columns = [];
    this.values = [];

    this.set = function(columns, values) {
        this.columns = columns;
        this.values = values;
        return this;
    };

    this.sql = function() {
        var q =  "INSERT INTO " + this.tableName + "(";
        q += this.columns.join(",");
        q += ") VALUES";

        var valuesList = [];
        var columnLength = this.columns.length;
        for (var i=0, length=this.values.length,
                 value=this.values[0]; i<length; i++,
            value=this.values[i]) {

            var vals = [];
            for (var j=0; j<columnLength; j++) {
                var col = this.columns[j];
                var val = this.builder.escape(value[col] || null);
                vals.push(val);
            }
            valuesList.push("(" + vals.join(",") + ")");
        }
        q += valuesList.join(",");
        return q;
    };

    this.exec = function(cb) {
        this.builder.exec(this.sql(), cb);
    };
};


/*
 Chainable query builder interface for building
 Insert statements.
 */
var UpdateBuilder = function(pool, tableName) {
    this.builder = new QueryBuilder(pool);
    this.tableName = tableName;

    this.set = function(obj, exclude) {
        this.builder.set(obj, exclude);
        return this;
    };

    this.where = function(field, value, operator) {
        this.builder.where(field, value, operator);
        return this;
    };

    this.sql = function() {
        var query = "UPDATE " + this.tableName;
        query += this.builder.setSegment;
        query += this.builder.whereSegment;
        return query;
    };

    this.exec = function(cb) {
        this.builder.exec(this.sql(), cb);
    };
};

var SelectBuilder = function(pool, tableName) {
    this.builder = new QueryBuilder(pool);
    this.tableName = tableName;

    this.fields = function(columns) {
        this.columns = columns;
        return this;
    };

    this.where = function(field, value, operator) {
        this.builder.where(field, value, operator);
        return this;
    };

    this.join = function(field1, field2) {
        this.builder.join(field1, field2);
        return this;
    };

    this.leftJoin = function(table, field1, field2) {
        this.builder.leftJoin(table, field1, field2);
        return this;
    };

    this.innerJoin = function(table, field1, field2) {
        this.builder.innerJoin(table, field1, field2);
        return this;
    };

    this.groupBy = function(fields) {
        this.builder.groupBy(fields);
        return this;
    };

    this.orderBy = function(fields, asc) {
        this.builder.orderBy(fields, asc);
        return this;
    };

    this.limit = function(rows) {
        this.builder.limit(rows);
        return this;
    };

    this.sql = function() {
        var query = "SELECT " + (this.columns ? this.columns.join(',') : '*');
        query += " FROM " + this.tableName;
        query += this.builder.joinSegment;
        query += this.builder.whereSegment;
        query += this.builder.groupBySegment;
        query += this.builder.orderBySegment;
        query += this.builder.limitSegment;

        return query;
    };

    this.exec = function(cb) {
        this.builder.exec(this.sql(), cb);
    }
};

var DeleteBuilder = function(pool, tableName) {
    this.builder = new QueryBuilder(pool);
    this.tableName = tableName;

    this.where = function(field, value, operator) {
        this.builder.where(field, value, operator);
        return this;
    }

    this.sql = function() {
        var query = "DELETE FROM " + this.tableName;
        query += this.builder.whereSegment;
        return query;
    }

    this.exec = function(cb) {
        this.builder.exec(this.sql(), cb);
    }
};

/*
 The Db class. Provides a unified interface to the
 query builders.
 */
module.exports = Db = function(poolOpts) {

    var self = this;

    /*
     Lazily initialized sql connection object.
     */
    this.pool = function() {

        if (! this.poolInstance) {
            this.poolInstance  = mysql.createPool(poolOpts);
        }

        return this.poolInstance;
    };

    /*
     Returns an update builder object.

     @param tableName {String} table to update.

     Example:

     db.update("Customer")
     .set({
     name: "John Doe",
     phone: "9292929292",
     dateOfBirth: "12/12/1990"
     age: 21
     }, exclude: ["age"])
     .where("id", 12)
     .exec(function(err, info) {
     // Process result
     // ...
     });
     */
    this.update = function(tableName) {
        return new UpdateBuilder(self.pool(), tableName);
    };

    /*
     Returns an insert builder object.

     @param tableName {String} table to insert.

     Example:

     db.insertInto("Customer")
     .set({
     name: "John Doe",
     phone: "9292929292",
     dateOfBirth: "12/12/1990"
     age: 21
     }, exclude: ["age"])
     .exec(function(err, info) {
     // Process result
     // ...
     });
     */
    this.insertInto = function(tableName) {
        return new InsertBuilder(self.pool(), tableName);
    };

    /*
     Returns an multiple insert builder object.

     @param tableName {String} table to update.

     Example:

     db.insertMultipleRowsInto("Customer")
     .set(["name", "phone", "dateOfBirth"], [{
     name: "John Doe",
     phone: "9292929292",
     dateOfBirth: "12/12/1990"
     age: 21
     },{
     name: "Jane Doe",
     phone: "9191919191",
     dateOfBirth: "1/1/2000"
     age: 11
     }])
     .exec(function(err, info) {
     // Process result
     // ...
     });
     */
    this.insertMultipleRowsInto = function(tableName) {
        return new MultiInsertBuilder(self.pool(), tableName);
    };

    this.selectFrom = function(tableName) {
        return new SelectBuilder(self.pool(), tableName);
    };

    this.deleteFrom = function(tableName) {
        return new DeleteBuilder(self.pool(), tableName);
    };

    /*
     Executes a well formed sql query.
     */
    // this.execSql = function(sql, cb) {
    //     self.pool().getConnection(function(err, conn) {
    //         if (err) return cb(err);
    //
    //         conn.query(sql, function(err, rows) {
    //             conn.release();
    //
    //             cb(err, rows);
    //         });
    //     });
    // };

    this.execSql = function(sql, values, cb) {
        if (typeof cb == "undefined") {
            cb = values;
            values = undefined;
        }

        self.pool().getConnection(function(err, conn) {
            if (err) return cb(err);

            function callback(err, rows) {
                conn.release();
                cb(err, rows);
            }

            if (values) {
                conn.query(sql, values, callback);
            } else {
                conn.query(sql, callback);
            }
        });
    };


    /*
     Escapes the specified value, for safety against SQL injection.
     */
    this.escape = function (value) {
        return mysql.escape(value);
    };

    /*
     * Returns the connection object.
     * @param cb  callback that is called with err, connection
     */
    this.getConnection = function (cb) {
        self.pool().getConnection(cb);
    };
};