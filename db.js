var mysql = require('mysql');
var pool = false;

var runQuery = (query, vars, cb) => {
  if(pool) {
    pool.getConnection((err, conn) => {
      if(err) {
        console.log(err);
        cb(false);
      }
      
      conn.query(query, vars, (err, results) => {
        conn.release();
        if(err) {
          console.log("ERROR - " + err);
          cb(err);
        }
        cb(results);
      });
    });
  }
};

var defaults = {
  connectionLimit: 100,
  host: '127.0.0.1',
  user: 'root',
  password: '',
  database: 'database',
  debug: true
}

var setup = (options) => {
  var options = Object.assign(defaults, options);
  pool = mysql.createPool(options);
};

var select = (options, array, log) => {
  if(typeof log == "undefined") {
    log = false;
  }
  var cols = "*";
  var where = "";
  var variables = [];
  if(typeof options.columns != "undefined") {
    cols = options.columns.join();
  }
      
  if(typeof options.conditions != "undefined") {
    for(var i=0; i<options.conditions.length; i++) {
      if(i != 0) {
        where += " AND ";
      }
      if(options.conditions[i].length == 2) {
        where += options.conditions[i][0] + " = ?";;
        variables.push(options.conditions[i][1]);
      } else if(options.conditions[i].length == 3) {
        where += options.conditions[i][0] + " " + options.conditions[i][1] + " ?";
        variables.push(options.conditions[i][2]);
      } else {
        break;
      }
    }
  }
  var sql = "SELECT " + cols + " FROM " + options.table;
  if(typeof options.join != "undefined") {
    for(var i=0; i<options.join.length; i++) {
      sql += " " + options.join[i].type + " " + options.join[i].table + " ON ";
      if(options.join[i].on.length == 2) {
        sql += options.join[i].on[0] + " = " + options.join[i].on[1];
      } else if(options.join[i].on.length == 3) {
        sql += options.join[i].on[0] + " " + options.join[i].on[1] + " " + options.join[i].on[2];
      } else {
        break;
      }
    }
  }
  if(where != "") {
    sql += " WHERE " + where;
  }
  if(typeof options.order != "undefined") {
    sql += " ORDER BY ";
    for(var i=0; i<options.order.length; i++) {
      if(i > 0) {
         sql += ", ";
       }
       sql += options.order[i][0] + " " + options.order[i][1];
    }
  }
  if(typeof options.limit != "undefined") {
    sql += " LIMIT " + options.limit[0];
    if(options.limit.length > 1) {
       sql += "," + options.limit[1];
    }
  }
  sql += ";";
  if(log) {
    console.log(sql);
    console.log(variables);
  }
  return new Promise((resolve, reject) => {
    runQuery(sql, variables, (res) => {
      if(res) {
        if(res.length == 1 && array != true) {
          resolve(res[0])
        } else {
          resolve(res)
        }
      } else {
        reject();
       }
    });
  });
};

var insert = (options) => {      
  var cols = "";
  var variables = [];
  if(typeof options.columns != "undefined") {
    cols = options.columns.join();
  }
    
  var sql = "INSERT INTO " + options.table + "(" + cols + ") VALUES ";
  var vars = options.variables;
  if(vars[0].constructor !== Array) {
    vars = [];
    vars[0] = options.variables;
  }
  for(var i=0; i<vars.length; i++) {
    if(i != 0) {
      sql += ",";
    }
    sql += "(";
    for(var j=0; j<vars[i].length; j++) {
      if(j != 0) {
        sql += ",";
      }
      sql += "?";
      variables.push(vars[i][j]);
    }
    sql += ")";
  }
  sql += ";";
  return new Promise((resolve, reject) => {
    runQuery(sql, variables, (res) => {
      if(res) {
        var insertId = res.insertId;
        if(res.affectedRows > 1) {
          insertId = [res.insertId];
          for(var i = 1; i < res.affectedRows; i++) {
            insertId.push(res.insertId + i);
          }
        }
        resolve(insertId)
      } else {
        reject();
      }
    });
  });
};

var update = (options) => {  
  var where = "";
  var variables = [];
  var error = false;
  if(options.columns.length != options.variables.length) {
    console.log('Error - variables count doesn\'t match column count');
    error = true;
  }
      
  var sql = "UPDATE " + options.table + " SET ";
  var columns = options.columns;
  var vars = options.variables;
  for(var i=0; i<columns.length; i++) {
    if(i != 0) {
      sql += ", ";
    }
    sql += columns[i] + " = ?";
    variables.push(vars[i]);
  }
  if(typeof options.conditions != "undefined") {
    for(var i=0; i<options.conditions.length; i++) {
      if(i != 0) {
        where += " AND ";
      }
      if(options.conditions[i].length == 2) {
        where += options.conditions[i][0] + " = ?";;
        variables.push(options.conditions[i][1]);
      } else if(options.conditions[i].length == 3) {
        where += options.conditions[i][0] + " " + options.conditions[i][1] + " ?";
        variables.push(options.conditions[i][2]);
      } else {
        break;
      }
    }
  }
  if(where != "") {
    sql += " WHERE " + where;
  }
  sql += ";";
  return new Promise((resolve, reject) => {
    if(error) {
      reject();
    }
    runQuery(sql, variables, (res) => {
      if(res) {
        console.log(res);
      } else {
        reject();
      }
    });
  });
};

var query = (options) => {
  return new Promise((resolve, reject) => {
    runQuery(options.sql, options.variables, (res) => {
      if(res) {
          resolve(res);
        } else {
          reject();
        }
    });
  });
};

var getPool = () => {
  return pool;
};

module.exports = {
  "setup": setup,
  "select": select,
  "insert": insert,
  "update": update,
  "query": query,
  "getPool": getPool
}