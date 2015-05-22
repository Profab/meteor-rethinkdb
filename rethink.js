var Future = Npm.require('fibers/future');
var url = Npm.require('url');
var r = Rethink.r;

var writeMethods = [
  'insert',
  'update',
  'replace',
  'delete'
];

var rethinkUrl = process.env.RETHINK_URL;

var parsedConnectionUrl = url.parse(rethinkUrl || 'rethinkdb://localhost:28015/test');
var connection = r.connect({
  host: parsedConnectionUrl.hostname || 'localhost',
  port: parsedConnectionUrl.port || '28015',
  db: (parsedConnectionUrl.pathname || '/test').split('/')[1],
  authKey: (parsedConnectionUrl.query || {}).authKey
});

try {
  connection = wait(connection);
} catch (err) {
  throw new Error(
    "Error connecting to RethinkDB: " + err.message + "\n\n" +
    "Set the RETHINK_URL environment variable. Example: rethinkdb://localhost:28015/database?authKey=somekey"
  );
}
Rethink._connection = connection;

Rethink.Table = function (name, options) {
  var self = this;
  options = options || {};

  self.name = name;
  self._prefix = '/' + name + '/';
  self._dbConnection = options.dbConnection || connection;
  self._connection = options.connection || Meteor.server;
  self._synthEventCallbacks = [];
  self._lastSynthEvent = 0;

  Rethink.Table._checkName(name);

  // define an RPC end-point
  var methods = {};
  methods[self._prefix + 'run'] = function (builtQuery, generatedKeys) {
    function FakeQuery(serializedQuery) {
      this.parsed = serializedQuery;
      this.tt = this.parsed[0];
      this.queryString = ":(";
    }
    FakeQuery.prototype.build = function () { return this.parsed; };

    var f = new Future;
    self._dbConnection._start(new FakeQuery(builtQuery), f.resolver(), {});
    return f.wait();
  };
  self._connection.methods(methods);
};


// These injected methods _getCollectionName are to allow
// publishing arrays of cursors
Rethink.Table.prototype._getCollectionName = function () {
  return this.name;
};

attachCursorMethod('_getCollectionName', function () {
  return function () {
    return this._table.name;
  };
});

Rethink.Table._checkName = function (name) {
  var tables = r.tableList().run(connection);
  if (tables.indexOf(name) === -1)
    throw new Error("The table '" + name + "' doesn't exist in your RethinkDB database.");
};

Rethink.Table.prototype._deregisterMethods = function () {
  var self = this;
  delete self._connection.method_handlers[self._prefix + 'run'];
};


///////////////////////////////////////////////////////////////////////////////
// Monkey-patching section
///////////////////////////////////////////////////////////////////////////////
wrapCursorMethods(function (ret, m) {
  ret._connection = this._connection;
  ret._table = this._table;
  ret._writeQuery = this._writeQuery || writeMethods.indexOf(m) !== -1;
});
wrapTableMethods(function (ret, m) {
  ret._connection = this._dbConnection;
  ret._table = this;
  ret._writeQuery = writeMethods.indexOf(m) !== -1;
}, Rethink.Table.prototype);

// monkey patch `run()`
var originalRun;
attachCursorMethod('run', function (proto) {
  originalRun = proto.run;
  return function (conn, callback) {
    if (! conn || typeof conn === 'function') {
      callback = callback || conn;
      conn = this._connection;
    }

    var future = null;
    if (! callback) {
      future = new Future();
      callback = future.resolver();
    }

    if (this._writeQuery && DDPServer._CurrentWriteFence.get()) {
      var table = this._table;
      var write = DDPServer._CurrentWriteFence.get().beginWrite();
      var origCb = callback;
      var id = Math.random();
      callback = function (err, res) {
        if (! err) {
          // release this write only after we are sure the event was processed
          registerSyntheticEvent(table, function () {
            write.committed();
          });
        } else {
          write.committed();
        }
        origCb(err, res);
      };
    }

    callback = Meteor.bindEnvironment(callback);

    originalRun.call(this, conn, callback);
    if (future)
      return future.wait();
  };
});
attachCursorMethod('_run', function () {
  return originalRun;
});

///////////////////////////////////////////////////////////////////////////////
// Extra cursor methods as syntactic sugar
///////////////////////////////////////////////////////////////////////////////
attachCursorMethod('fetch', function () {
  return function () {
    var self = this;
    return wait(self.run().toArray());
  };
});

var SYNTH_EVENT_ID = 'meteor-rethink-synthetic-event';
var registerSyntheticEvent = function (table, cb) {
  var synthInsert = wait(table.insert({
    id: SYNTH_EVENT_ID,
    ts: r.now()
  }, {
    returnChanges: true,
    conflict: 'replace'
  })._run(table._dbConnection));

  var ts;
  var change = synthInsert.changes[0];
  if (change.new_val)
    ts = change.new_val.ts;
  else if (change.old_val)
    ts = change.old_val.ts;
  else
    throw new Error('Error in Rethink-Meteor: unexpected changes field: ' + JSON.stringify(change));

  if (table._lastSynthEvent >= ts) {
    Meteor.defer(cb);
    return;
  }

  table._synthEventCallbacks.push({
    f: cb,
    ts: ts
  });
};

var changeFeedHandler = function (err, notif) {
  var cbs = this;
  if (!err) {
    if (notif.old_val === undefined && notif.new_val === null) {
      // nothing found
      return;
    }
    if (! notif.old_val) {
      cbs.added(notif.new_val);
      return;
    }
    if (! notif.new_val) {
      cbs.removed(notif.old_val);
      return;
    }
    if (notif.new_val.id === notif.old_val.id) {
      cbs.changed(notif.new_val, notif.old_val);
      return;
    }

    // one val was removed, another was added
    cbs.removed(notif.old_val);
    cbs.added(notif.new_val);
  } else {
    cbs.error(err);
  }
};

var observe = function (callbacks) {
  var cbs = {
    added: callbacks.added || function () {},
    changed: callbacks.changed || function () {},
    removed: callbacks.removed || function () {},
    error: callbacks.error || function (err) { throw err; }
  };

  var self = this;
  var streamCursor;
  var initValuesFuture = new Future();
  
  // Get initial results first
  // XXX Need to handle queries that literally don't return cursors
  // but rather return single documents
  try {
    var initialResult = self.run();
    // Check if it's iterable, if not, it means it's a single document
    // returned by a query like .min() or .max()
    if (_.isFunction(initialResult.each)) {
      initialResult.each(Meteor.bindEnvironment(function (err, doc) {
        if (!err) {
          cbs.added(doc);
        } else {
          initialResult.close();
          initValuesFuture.isResolved() ? cbs.error(err) : initValuesFuture.throw(err);
        }
      }), Meteor.bindEnvironment(function () {
        // This callback is the onFinished callback that gets called
        // after we're done iterating the cursor.
        // This is also the spot to resolve the future, so that the observer can finish blocking
        if (!initValuesFuture.isResolved()) {
          initValuesFuture.return();
        }
        // After all initial values have been gotten, then we can create
        // our awesome change feeds! Yea!
        streamCursor = self.changes().run();
        streamCursor.each(Meteor.bindEnvironment(changeFeedHandler.bind(cbs)));
      }));
    } else {
      // Handle single point queries here.
      // Because they give you an initial value, there's
      // no need to get it and return it on .added
      var initializing = true;
      if (initialResult) {
        // Send the initial value here
        cbs.added(initialResult);
      }
      // Unblock
      initValuesFuture.return();
      // Start the change-feed
      streamCursor = self.changes().run();
      streamCursor.each(Meteor.bindEnvironment(function (err, notif) {
        if (err) {
          initValuesFuture.isResolved() ? cbs.error(err) : initValuesFuture.throw(err);
        } else if (initializing) {
          // This skips the initial value
          // Assuming that it is
          initializing = false;
        } else {
          changeFeedHandler.apply(cbs, arguments);  
        }
      }));
    }
    
      
    // This is to make it work like a regular
    // Mongo observe, where it blocks on the initial
    // values, maybe this might change?
    // I suppose that if you don't want your observe to not
    // block, you can always wrap it in a Meteor.defer
    initValuesFuture.wait();
    
    return {
      stop: function () {
        streamCursor && wait(streamCursor.close());
      }
    };
  } catch (e) {
    initValuesFuture.isResolved() ? cbs.error(e) : initValuesFuture.throw(e);
  }
};

attachCursorMethod('observe', function () {
  return observe;
});

Rethink.Table.prototype._publishCursor = function (sub) {
  var self = this;
  return self.filter({})._publishCursor(sub);
};

attachCursorMethod('_publishCursor', function () {
  return function (sub) {
    var self = this;
    try {
      Rethink.Table._publishCursor(self, sub, self._table.name);
    } catch (err) {
      sub.error(err);
    }
  };
});


Rethink.Table._publishCursor = function (cursor, sub, tableName) {
  var observeHandle = cursor.observe({
    added: function (doc) {
      sub.added(tableName, doc.id, doc);
    },
    changed: function (newDoc, oldDoc) {
      var fields = diffObject(oldDoc, newDoc);
      sub.changed(tableName, newDoc.id, fields);
    },
    removed: function (doc) {
      sub.removed(tableName, doc.id);
    }
  });

  // We don't call sub.ready() here: it gets called in livedata_server, after
  // possibly calling _publishCursor on multiple returned cursors.

  // register stop callback (expects lambda w/ no args).
  sub.onStop(function () {
    observeHandle && observeHandle.stop();
  });
};

function diffObject (oldDoc, newDoc) {
  var diff = {};
  Object.keys(newDoc).forEach(function (property) {
    if (! EJSON.equals(oldDoc[property], newDoc[property]))
      diff[property] = newDoc[property];
  });
  Object.keys(oldDoc).forEach(function (property) {
    if (! newDoc.hasOwnProperty(property))
      diff[property] = undefined;
  });

  return diff;
}

function wait (promise, after) {
  var f = new Future;
  promise.then(Meteor.bindEnvironment(function (res) {
    f.return(res);
  }), Meteor.bindEnvironment(function (err) {
    f.throw(err);
  }));

  var res = f.wait();
  if (after)
    after();
  return res;
}

