//var assert = function
var assert = function(x){
    if(!x){
        throw new Error("assert failed:", x);
    }
}
print = console.log

var NumberLong=  function(){}
var NumberInt=  function(){}
var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");

var ObjectId = function ObjectId(hex) {
  if(!(this instanceof ObjectId)) return new ObjectId(hex);
    if(!hex){
        return new ObjectId(ObjectId.generate())
    }else{
        var parsed = ObjectId.parseHexString(hex);
        this.unixTime = parsed[0]
        this.machineId = parsed[1]
        this.processId = parsed[2]
        this.counter = parsed[3]
        this.str = this.valueOf();
    }
}

ObjectId.MACHINE_ID = parseInt(Math.random() * 0xFFFFFF, 10);
ObjectId.PROCESS_ID = parseInt(Math.random() * 0xFFFF, 10);
ObjectId.index = parseInt(Math.random() * 0xFFFFFF, 10);

ObjectId.get_inc = function() {
  return ObjectId.index = (ObjectId.index + 1) % 0xFFFFFF;
};

ObjectId.pad = function(str, length, ch) {
    var result = str;
    if(!ch) ch = '0'
    while(result.length<length){
        result = ch + result
    }
    return result;
}


ObjectId.decodeHex = function(str, bigEndian){
    var numBytes = str.length / 2;
    var result = 0;
    var bitdir = bigEndian ? -1 : 1;
    for(var i=0, bitnum = bigEndian ? numBytes : 0; 
    i<str.length;
    i+=2, bitnum += bigEndian ? -1 : 1){
        var num = parseInt(str.substring(i, i+2), 16)
        result += num << ((bitnum-1) * 8)
    }
    return result
}

ObjectId.parseHexString = function(hexString){
    if(!checkForHexRegExp.test(hexString)){
        throw new Error("invalid object id: not hex");
    }else{
        var unixTime = ObjectId.decodeHex(hexString.substring(0, 8), true);
        var machineId = ObjectId.decodeHex(hexString.substring(8, 14), true);
        var processId = ObjectId.decodeHex(hexString.substring(14, 18), true);
        var counter = ObjectId.decodeHex(hexString.substring(18), true);
        return [unixTime, machineId, processId, counter];
    }
}

ObjectId.generate = function(){
    var unixTime = Number(parseInt(Date.now()/1000,10)).toString(16);
    var machine_id = Number(ObjectId.MACHINE_ID).toString(16); // 3 bytes
    var process_id = Number(ObjectId.PROCESS_ID).toString(16); // 2 bytes
    var counter = Number(ObjectId.get_inc()).toString(16); // 3 bytes
    return ObjectId.pad(unixTime, 8) +
           ObjectId.pad(machine_id, 6) + 
           ObjectId.pad(process_id, 4) + 
           ObjectId.pad(counter, 6);
}

ObjectId.prototype.getTimestamp = function(){
    return this.timestamp;
}

ObjectId.prototype.toString = function(){
    return "ObjectId('" + this.valueOf() + "')";
}

ObjectId.prototype.valueOf = function(){
    var unixTime16 = Number(this.unixTime).toString(16);
    var machine_id16 = Number(this.machineId).toString(16); // 3 bytes
    var process_id16 = Number(this.processId).toString(16); // 2 bytes
    var counter = Number(this.counter).toString(16); // 3 bytes
    return ObjectId.pad(unixTime16 , 8) +
           ObjectId.pad(machine_id16, 6) + 
           ObjectId.pad(process_id16, 4) + 
           ObjectId.pad(counter, 6);
}
_parsePath = function() {
    var dbpath = "";
    for( var i = 0; i < arguments.length; ++i )
        if ( arguments[ i ] == "--dbpath" )
            dbpath = arguments[ i + 1 ];

    if ( dbpath == "" )
        throw "No dbpath specified";

    return dbpath;
}

_parsePort = function() {
    var port = "";
    for( var i = 0; i < arguments.length; ++i )
        if ( arguments[ i ] == "--port" )
            port = arguments[ i + 1 ];

    if ( port == "" )
        throw "No port specified";
    return port;
}



__nextPort = 27000;
myPort = function() {
    var m = db.getMongo();
    if ( m.host.match( /:/ ) )
        return m.host.match( /:(.*)/ )[ 1 ];
    else
        return 27017;
}
// mongo.js

// NOTE 'Mongo' may be defined here or in MongoJS.cpp.  Add code to init, not to this constructor.
if ( typeof Mongo == "undefined" ){
    Mongo = function( host ){
        if(!host){
            host = 'localhost'
        }
        this.host = host;
    }
}

if ( ! Mongo.prototype ){
    throw "Mongo.prototype not defined";
}

if ( ! Mongo.prototype.find )
    Mongo.prototype.find = function( ns , query , fields , limit , skip , batchSize , options ){ throw "find not implemented"; }
if ( ! Mongo.prototype.insert )
    Mongo.prototype.insert = function( ns , obj ){ throw "insert not implemented"; }
if ( ! Mongo.prototype.remove )
    Mongo.prototype.remove = function( ns , pattern ){ throw "remove not implemented;" }
if ( ! Mongo.prototype.update )
    Mongo.prototype.update = function( ns , query , obj , upsert ){ throw "update not implemented;" }

if ( typeof mongoInject == "function" ){
    mongoInject( Mongo.prototype );
}

Mongo.prototype.setSlaveOk = function( value ) {
    if( value == undefined ) value = true;
    this.slaveOk = value;
}

Mongo.prototype.getSlaveOk = function() {
    return this.slaveOk || false;
}

Mongo.prototype.getDB = function( name ){
    if (jsTest.options().keyFile && ((typeof this.authenticated == 'undefined') || !this.authenticated)) {
        jsTest.authenticate(this)
    }
    return new DB( this , name );
}

Mongo.prototype.getDBs = function(){
    var res = this.getDB( "admin" ).runCommand( { "listDatabases" : 1 } );
    if ( ! res.ok )
        throw "listDatabases failed:" + tojson( res );
    return res;
}

Mongo.prototype.adminCommand = function( cmd ){
    return this.getDB( "admin" ).runCommand( cmd );
}

Mongo.prototype.setLogLevel = function( logLevel ){
    return this.adminCommand({ setParameter : 1, logLevel : logLevel })
}

Mongo.prototype.getDBNames = function(){
    return this.getDBs().databases.map( 
        function(z){
            return z.name;
        }
    );
}

Mongo.prototype.getCollection = function(ns){
    var idx = ns.indexOf( "." );
    if ( idx < 0 ) 
        throw "need . in ns";
    var db = ns.substring( 0 , idx );
    var c = ns.substring( idx + 1 );
    return this.getDB( db ).getCollection( c );
}

Mongo.prototype.toString = function(){
    return "connection to " + this.host;
}
Mongo.prototype.tojson = Mongo.prototype.toString;

connect = function( url , user , pass ){
    chatty( "connecting to: " + url )

    if ( user && ! pass )
        throw "you specified a user and not a password.  either you need a password, or you're using the old connect api";

    var idx = url.lastIndexOf( "/" );
    
    var db;
    
    if ( idx < 0 )
        db = new Mongo().getDB( url );
    else 
        db = new Mongo( url.substring( 0 , idx ) ).getDB( url.substring( idx + 1 ) );
    
    if ( user && pass ){
        if ( ! db.auth( user , pass ) ){
            throw "couldn't login";
        }
    }
    
    return db;
}
// @file collection.js - DBCollection support in the mongo shell
// db.colName is a DBCollection object
// or db["colName"]

if ( ( typeof  DBCollection ) == "undefined" ){
    DBCollection = function( mongo , db , shortName , fullName ){
        this._mongo = mongo;
        this._db = db;
        this._shortName = shortName;
        this._fullName = fullName;

        this.verify();
    }
}

DBCollection.prototype.verify = function(){
    assert( this._fullName , "no fullName" );
    assert( this._shortName , "no shortName" );
    assert( this._db , "no db" );

    assert.eq( this._fullName , this._db._name + "." + this._shortName , "name mismatch" );

    assert( this._mongo , "no mongo in DBCollection" );
}

DBCollection.prototype.getName = function(){
    return this._shortName;
}

DBCollection.prototype.help = function () {
    var shortName = this.getName();
    print("DBCollection help");
    print("\tdb." + shortName + ".find().help() - show DBCursor help");
    print("\tdb." + shortName + ".count()");
    print("\tdb." + shortName + ".copyTo(newColl) - duplicates collection by copying all documents to newColl; no indexes are copied.");
    print("\tdb." + shortName + ".convertToCapped(maxBytes) - calls {convertToCapped:'" + shortName + "', size:maxBytes}} command");
    print("\tdb." + shortName + ".dataSize()");
    print("\tdb." + shortName + ".distinct( key ) - e.g. db." + shortName + ".distinct( 'x' )");
    print("\tdb." + shortName + ".drop() drop the collection");
    print("\tdb." + shortName + ".dropIndex(index) - e.g. db." + shortName + ".dropIndex( \"indexName\" ) or db." + shortName + ".dropIndex( { \"indexKey\" : 1 } )");
    print("\tdb." + shortName + ".dropIndexes()");
    print("\tdb." + shortName + ".ensureIndex(keypattern[,options]) - options is an object with these possible fields: name, unique, dropDups");
    print("\tdb." + shortName + ".reIndex()");
    print("\tdb." + shortName + ".find([query],[fields]) - query is an optional query filter. fields is optional set of fields to return.");
    print("\t                                              e.g. db." + shortName + ".find( {x:77} , {name:1, x:1} )");
    print("\tdb." + shortName + ".find(...).count()");
    print("\tdb." + shortName + ".find(...).limit(n)");
    print("\tdb." + shortName + ".find(...).skip(n)");
    print("\tdb." + shortName + ".find(...).sort(...)");
    print("\tdb." + shortName + ".findOne([query])");
    print("\tdb." + shortName + ".findAndModify( { update : ... , remove : bool [, query: {}, sort: {}, 'new': false] } )");
    print("\tdb." + shortName + ".getDB() get DB object associated with collection");
    print("\tdb." + shortName + ".getIndexes()");
    print("\tdb." + shortName + ".group( { key : ..., initial: ..., reduce : ...[, cond: ...] } )");
    print("\tdb." + shortName + ".insert(obj)");
    print("\tdb." + shortName + ".mapReduce( mapFunction , reduceFunction , <optional params> )");
    print("\tdb." + shortName + ".remove(query)");
    print("\tdb." + shortName + ".renameCollection( newName , <dropTarget> ) renames the collection.");
    print("\tdb." + shortName + ".runCommand( name , <options> ) runs a db command with the given name where the first param is the collection name");
    print("\tdb." + shortName + ".save(obj)");
    print("\tdb." + shortName + ".stats()");
    print("\tdb." + shortName + ".storageSize() - includes free space allocated to this collection");
    print("\tdb." + shortName + ".totalIndexSize() - size in bytes of all the indexes");
    print("\tdb." + shortName + ".totalSize() - storage allocated for all data and indexes");
    print("\tdb." + shortName + ".update(query, object[, upsert_bool, multi_bool]) - instead of two flags, you can pass an object with fields: upsert, multi");
    print("\tdb." + shortName + ".validate( <full> ) - SLOW");;
    print("\tdb." + shortName + ".getShardVersion() - only for use with sharding");
    print("\tdb." + shortName + ".getShardDistribution() - prints statistics about data distribution in the cluster");
    print("\tdb." + shortName + ".getSplitKeysForChunks( <maxChunkSize> ) - calculates split points over all chunks and returns splitter function");
    return __magicNoPrint;
}

DBCollection.prototype.getFullName = function(){
    return this._fullName;
}
DBCollection.prototype.getMongo = function(){
    return this._db.getMongo();
}
DBCollection.prototype.getDB = function(){
    return this._db;
}

DBCollection.prototype._dbCommand = function( cmd , params ){
    if ( typeof( cmd ) == "object" )
        return this._db._dbCommand( cmd );
    
    var c = {};
    c[cmd] = this.getName();
    if ( params )
        Object.extend( c , params );
    return this._db._dbCommand( c );    
}

DBCollection.prototype.runCommand = DBCollection.prototype._dbCommand;

DBCollection.prototype._massageObject = function( q ){
    if ( ! q )
        return {};

    var type = typeof q;

    if ( type == "function" )
        return { $where : q };

    if ( q.isObjectId )
        return { _id : q };

    if ( type == "object" )
        return q;

    if ( type == "string" ){
        if ( q.length == 24 )
            return { _id : q };

        return { $where : q };
    }

    throw "don't know how to massage : " + type;

}


DBCollection.prototype._validateObject = function( o ){
    if ( o._ensureSpecial && o._checkModify )
        throw "can't save a DBQuery object";
}

DBCollection._allowedFields = { $id : 1 , $ref : 1 , $db : 1 , $MinKey : 1, $MaxKey : 1 };

DBCollection.prototype._validateForStorage = function( o ){
    this._validateObject( o );
    for ( var k in o ){
        if ( k.indexOf( "." ) >= 0 ) {
            throw "can't have . in field names [" + k + "]" ;
        }

        if ( k.indexOf( "$" ) == 0 && ! DBCollection._allowedFields[k] ) {
            throw "field names cannot start with $ [" + k + "]";
        }

        if ( o[k] !== null && typeof( o[k] ) === "object" ) {
            this._validateForStorage( o[k] );
        }
    }
};


DBCollection.prototype.find = function( query , fields , limit , skip, batchSize, options ){
    return new DBQuery( this._mongo , this._db , this ,
                        this._fullName , this._massageObject( query ) , fields , limit , skip , batchSize , options || this.getQueryOptions() );
}

DBCollection.prototype.findOne = function( query , fields, options ){
    var cursor = this._mongo.find( this._fullName , this._massageObject( query ) || {} , fields , 
        -1 /* limit */ , 0 /* skip*/, 0 /* batchSize */ , options || this.getQueryOptions() /* options */ );
    if ( ! cursor.hasNext() )
        return null;
    var ret = cursor.next();
    if ( cursor.hasNext() ) throw "findOne has more than 1 result!";
    if ( ret.$err )
        throw "error " + tojson( ret );
    return ret;
}

DBCollection.prototype.insert = function( obj , _allow_dot ){
    if ( ! obj )
        throw "no object passed to insert!";
    if ( ! _allow_dot ) {
        this._validateForStorage( obj );
    }
    if ( typeof( obj._id ) == "undefined" && ! Array.isArray( obj ) ){
        var tmp = obj; // don't want to modify input
        obj = {_id: new ObjectId()};
        for (var key in tmp){
            obj[key] = tmp[key];
        }
    }
    this._db._initExtraInfo();
    this._mongo.insert( this._fullName , obj );
    this._lastID = obj._id;
    this._db._getExtraInfo("Inserted");
}

DBCollection.prototype.remove = function( t , justOne ){
    for ( var k in t ){
        if ( k == "_id" && typeof( t[k] ) == "undefined" ){
            throw "can't have _id set to undefined in a remove expression"
        }
    }
    this._db._initExtraInfo();
    this._mongo.remove( this._fullName , this._massageObject( t ) , justOne ? true : false );
    this._db._getExtraInfo("Removed");
}

DBCollection.prototype.update = function( query , obj , upsert , multi ){
    assert( query , "need a query" );
    assert( obj , "need an object" );

    var firstKey = null;
    for (var k in obj) { firstKey = k; break; }

    if (firstKey != null && firstKey[0] == '$') {
        // for mods we only validate partially, for example keys may have dots
        this._validateObject( obj );
    } else {
        // we're basically inserting a brand new object, do full validation
        this._validateForStorage( obj );
    }

    // can pass options via object for improved readability    
    if ( typeof(upsert) === 'object' ) {
        assert( multi === undefined, "Fourth argument must be empty when specifying upsert and multi with an object." );

        opts = upsert;
        multi = opts.multi;
        upsert = opts.upsert;
    }

    this._db._initExtraInfo();
    this._mongo.update( this._fullName , query , obj , upsert ? true : false , multi ? true : false );
    this._db._getExtraInfo("Updated");
}

DBCollection.prototype.save = function( obj ){
    if ( obj == null || typeof( obj ) == "undefined" ) 
        throw "can't save a null";

    if ( typeof( obj ) == "number" || typeof( obj) == "string" )
        throw "can't save a number or string"

    if ( typeof( obj._id ) == "undefined" ){
        obj._id = new ObjectId();
        return this.insert( obj );
    }
    else {
        return this.update( { _id : obj._id } , obj , true );
    }
}

DBCollection.prototype._genIndexName = function( keys ){
    var name = "";
    for ( var k in keys ){
        var v = keys[k];
        if ( typeof v == "function" )
            continue;
        
        if ( name.length > 0 )
            name += "_";
        name += k + "_";

        name += v;
    }
    return name;
}

DBCollection.prototype._indexSpec = function( keys, options ) {
    var ret = { ns : this._fullName , key : keys , name : this._genIndexName( keys ) };

    if ( ! options ){
    }
    else if ( typeof ( options ) == "string" )
        ret.name = options;
    else if ( typeof ( options ) == "boolean" )
        ret.unique = true;
    else if ( typeof ( options ) == "object" ){
        if ( options.length ){
            var nb = 0;
            for ( var i=0; i<options.length; i++ ){
                if ( typeof ( options[i] ) == "string" )
                    ret.name = options[i];
                else if ( typeof( options[i] ) == "boolean" ){
                    if ( options[i] ){
                        if ( nb == 0 )
                            ret.unique = true;
                        if ( nb == 1 )
                            ret.dropDups = true;
                    }
                    nb++;
                }
            }
        }
        else {
            Object.extend( ret , options );
        }
    }
    else {
        throw "can't handle: " + typeof( options );
    }
    /*
        return ret;

    var name;
    var nTrue = 0;
    
    if ( ! isObject( options ) ) {
        options = [ options ];
    }
    
    if ( options.length ){
        for( var i = 0; i < options.length; ++i ) {
            var o = options[ i ];
            if ( isString( o ) ) {
                ret.name = o;
            } else if ( typeof( o ) == "boolean" ) {
	        if ( o ) {
		    ++nTrue;
	        }
            }
        }
        if ( nTrue > 0 ) {
	    ret.unique = true;
        }
        if ( nTrue > 1 ) {
	    ret.dropDups = true;
        }
    }
*/
    return ret;
}

DBCollection.prototype.createIndex = function( keys , options ){
    var o = this._indexSpec( keys, options );
    this._db.getCollection( "system.indexes" ).insert( o , true );
}

DBCollection.prototype.ensureIndex = function( keys , options ){
    var name = this._indexSpec( keys, options ).name;
    this._indexCache = this._indexCache || {};
    if ( this._indexCache[ name ] ){
        return;
    }

    this.createIndex( keys , options );
    if ( this.getDB().getLastError() == "" ) {
	this._indexCache[name] = true;
    }
}

DBCollection.prototype.resetIndexCache = function(){
    this._indexCache = {};
}

DBCollection.prototype.reIndex = function() {
    return this._db.runCommand({ reIndex: this.getName() });
}

DBCollection.prototype.dropIndexes = function(){
    this.resetIndexCache();

    var res = this._db.runCommand( { deleteIndexes: this.getName(), index: "*" } );
    assert( res , "no result from dropIndex result" );
    if ( res.ok )
        return res;

    if ( res.errmsg.match( /not found/ ) )
        return res;

    throw "error dropping indexes : " + tojson( res );
}


DBCollection.prototype.drop = function(){
    if ( arguments.length > 0 )
        throw "drop takes no argument";
    this.resetIndexCache();
    var ret = this._db.runCommand( { drop: this.getName() } );
    if ( ! ret.ok ){
        if ( ret.errmsg == "ns not found" )
            return false;
        throw "drop failed: " + tojson( ret );
    }
    return true;
}

DBCollection.prototype.findAndModify = function(args){
    var cmd = { findandmodify: this.getName() };
    for (var key in args){
        cmd[key] = args[key];
    }

    var ret = this._db.runCommand( cmd );
    if ( ! ret.ok ){
        if (ret.errmsg == "No matching object found"){
            return null;
        }
        throw "findAndModifyFailed failed: " + tojson( ret );
    }
    return ret.value;
}

DBCollection.prototype.renameCollection = function( newName , dropTarget ){
    return this._db._adminCommand( { renameCollection : this._fullName , 
                                     to : this._db._name + "." + newName , 
                                     dropTarget : dropTarget } )
}

DBCollection.prototype.validate = function(full) {
    var cmd = { validate: this.getName() };

    if (typeof(full) == 'object') // support arbitrary options here
        Object.extend(cmd, full);
    else
        cmd.full = full;

    var res = this._db.runCommand( cmd );

    if (typeof(res.valid) == 'undefined') {
        // old-style format just put everything in a string. Now using proper fields

        res.valid = false;

        var raw = res.result || res.raw;

        if ( raw ){
            var str = "-" + tojson( raw );
            res.valid = ! ( str.match( /exception/ ) || str.match( /corrupt/ ) );

            var p = /lastExtentSize:(\d+)/;
            var r = p.exec( str );
            if ( r ){
                res.lastExtentSize = Number( r[1] );
            }
        }
    }

    return res;
}

DBCollection.prototype.getShardVersion = function(){
    return this._db._adminCommand( { getShardVersion : this._fullName } );
}

DBCollection.prototype.getIndexes = function(){
    return this.getDB().getCollection( "system.indexes" ).find( { ns : this.getFullName() } ).toArray();
}

DBCollection.prototype.getIndices = DBCollection.prototype.getIndexes;
DBCollection.prototype.getIndexSpecs = DBCollection.prototype.getIndexes;

DBCollection.prototype.getIndexKeys = function(){
    return this.getIndexes().map(
        function(i){
            return i.key;
        }
    );
}


DBCollection.prototype.count = function( x ){
    return this.find( x ).count();
}

/**
 *  Drop free lists. Normally not used.
 *  Note this only does the collection itself, not the namespaces of its indexes (see cleanAll).
 */
DBCollection.prototype.clean = function() {
    return this._dbCommand( { clean: this.getName() } );
}



/**
 * <p>Drop a specified index.</p>
 *
 * <p>
 * "index" is the name of the index in the system.indexes name field (run db.system.indexes.find() to
 *  see example data), or an object holding the key(s) used to create the index.
 * For example:
 *  db.collectionName.dropIndex( "myIndexName" );
 *  db.collectionName.dropIndex( { "indexKey" : 1 } );
 * </p>
 *
 * @param {String} name or key object of index to delete.
 * @return A result object.  result.ok will be true if successful.
 */
DBCollection.prototype.dropIndex =  function(index) {
    assert(index, "need to specify index to dropIndex" );
    var res = this._dbCommand( "deleteIndexes", { index: index } );
    this.resetIndexCache();
    return res;
}

DBCollection.prototype.copyTo = function( newName ){
    return this.getDB().eval(
        function( collName , newName ){
            var from = db[collName];
            var to = db[newName];
            to.ensureIndex( { _id : 1 } );
            var count = 0;

            var cursor = from.find();
            while ( cursor.hasNext() ){
                var o = cursor.next();
                count++;
                to.save( o );
            }

            return count;
        } , this.getName() , newName
    );
}

DBCollection.prototype.getCollection = function( subName ){
    return this._db.getCollection( this._shortName + "." + subName );
}

DBCollection.prototype.stats = function( scale ){
    return this._db.runCommand( { collstats : this._shortName , scale : scale } );
}

DBCollection.prototype.dataSize = function(){
    return this.stats().size;
}

DBCollection.prototype.storageSize = function(){
    return this.stats().storageSize;
}

DBCollection.prototype.totalIndexSize = function( verbose ){
    var stats = this.stats();
    if (verbose){
        for (var ns in stats.indexSizes){
            print( ns + "\t" + stats.indexSizes[ns] );
        }
    }
    return stats.totalIndexSize;
}


DBCollection.prototype.totalSize = function(){
    var total = this.storageSize();
    var mydb = this._db;
    var shortName = this._shortName;
    this.getIndexes().forEach(
        function( spec ){
            var coll = mydb.getCollection( shortName + ".$" + spec.name );
            var mysize = coll.storageSize();
            //print( coll + "\t" + mysize + "\t" + tojson( coll.validate() ) );
            total += coll.dataSize();
        }
    );
    return total;
}


DBCollection.prototype.convertToCapped = function( bytes ){
    if ( ! bytes )
        throw "have to specify # of bytes";
    return this._dbCommand( { convertToCapped : this._shortName , size : bytes } )
}

DBCollection.prototype.exists = function(){
    return this._db.system.namespaces.findOne( { name : this._fullName } );
}

DBCollection.prototype.isCapped = function(){
    var e = this.exists();
    return ( e && e.options && e.options.capped ) ? true : false;
}

DBCollection.prototype._distinct = function( keyString , query ){
    return this._dbCommand( { distinct : this._shortName , key : keyString , query : query || {} } );
    if ( ! res.ok )
        throw "distinct failed: " + tojson( res );
    return res.values;
}

DBCollection.prototype.distinct = function( keyString , query ){
    var res = this._distinct( keyString , query );
    if ( ! res.ok )
        throw "distinct failed: " + tojson( res );
    return res.values;
}


DBCollection.prototype.aggregate = function( ops ) {
    
    var arr = ops;
    
    if ( ! ops.length ) {
        arr = [];
        for ( var i=0; i<arguments.length; i++ ) {
            arr.push( arguments[i] )
        }
    }
    
    return this.runCommand( "aggregate" , { pipeline : arr } );
}

DBCollection.prototype.group = function( params ){
    params.ns = this._shortName;
    return this._db.group( params );
}

DBCollection.prototype.groupcmd = function( params ){
    params.ns = this._shortName;
    return this._db.groupcmd( params );
}

MapReduceResult = function( db , o ){
    Object.extend( this , o );
    this._o = o;
    this._keys = Object.keySet( o );
    this._db = db;
    if ( this.result != null ) {
        this._coll = this._db.getCollection( this.result );
    }
}

MapReduceResult.prototype._simpleKeys = function(){
    return this._o;
}

MapReduceResult.prototype.find = function(){
    if ( this.results )
        return this.results;
    return DBCollection.prototype.find.apply( this._coll , arguments );
}

MapReduceResult.prototype.drop = function(){
    if ( this._coll ) {
        return this._coll.drop();
    }
}

/**
* just for debugging really
*/
MapReduceResult.prototype.convertToSingleObject = function(){
    var z = {};
    var it = this.results != null ? this.results : this._coll.find();
    it.forEach( function(a){ z[a._id] = a.value; } );
    return z;
}

DBCollection.prototype.convertToSingleObject = function(valueField){
    var z = {};
    this.find().forEach( function(a){ z[a._id] = a[valueField]; } );
    return z;
}

/**
* @param optional object of optional fields;
*/
DBCollection.prototype.mapReduce = function( map , reduce , optionsOrOutString ){
    var c = { mapreduce : this._shortName , map : map , reduce : reduce };
    assert( optionsOrOutString , "need to supply an optionsOrOutString" )

    if ( typeof( optionsOrOutString ) == "string" )
        c["out"] = optionsOrOutString;
    else
        Object.extend( c , optionsOrOutString );

    var raw = this._db.runCommand( c );
    if ( ! raw.ok ){
        __mrerror__ = raw;
        throw "map reduce failed:" + tojson(raw);
    }
    return new MapReduceResult( this._db , raw );

}

DBCollection.prototype.toString = function(){
    return this.getFullName();
}

DBCollection.prototype.toString = function(){
    return this.getFullName();
}


DBCollection.prototype.tojson = DBCollection.prototype.toString;

DBCollection.prototype.shellPrint = DBCollection.prototype.toString;

DBCollection.autocomplete = function(obj){
    var colls = DB.autocomplete(obj.getDB());
    var ret = [];
    for (var i=0; i<colls.length; i++){
        var c = colls[i];
        if (c.length <= obj.getName().length) continue;
        if (c.slice(0,obj.getName().length+1) != obj.getName()+'.') continue;

        ret.push(c.slice(obj.getName().length+1));
    }
    return ret;
}


// Sharding additions

/* 
Usage :

mongo <mongos>
> load('path-to-file/shardingAdditions.js')
Loading custom sharding extensions...
true

> var collection = db.getMongo().getCollection("foo.bar")
> collection.getShardDistribution() // prints statistics related to the collection's data distribution

> collection.getSplitKeysForChunks() // generates split points for all chunks in the collection, based on the
                                     // default maxChunkSize or alternately a specified chunk size
> collection.getSplitKeysForChunks( 10 ) // Mb

> var splitter = collection.getSplitKeysForChunks() // by default, the chunks are not split, the keys are just
                                                    // found.  A splitter function is returned which will actually
                                                    // do the splits.
                                                    
> splitter() // ! Actually executes the splits on the cluster !
                                                    
*/

DBCollection.prototype.getShardDistribution = function(){

   var stats = this.stats()
   
   if( ! stats.sharded ){
       print( "Collection " + this + " is not sharded." )
       return
   }
   
   var config = this.getMongo().getDB("config")
       
   var numChunks = 0
   
   for( var shard in stats.shards ){
       
       var shardDoc = config.shards.findOne({ _id : shard })
       
       print( "\nShard " + shard + " at " + shardDoc.host ) 
       
       var shardStats = stats.shards[ shard ]
               
       var chunks = config.chunks.find({ _id : sh._collRE( this ), shard : shard }).toArray()
       
       numChunks += chunks.length
       
       var estChunkData = shardStats.size / chunks.length
       var estChunkCount = Math.floor( shardStats.count / chunks.length )
       
       print( " data : " + sh._dataFormat( shardStats.size ) +
              " docs : " + shardStats.count +
              " chunks : " +  chunks.length )
       print( " estimated data per chunk : " + sh._dataFormat( estChunkData ) )
       print( " estimated docs per chunk : " + estChunkCount )
       
   }
   
   print( "\nTotals" )
   print( " data : " + sh._dataFormat( stats.size ) +
          " docs : " + stats.count +
          " chunks : " +  numChunks )
   for( var shard in stats.shards ){
   
       var shardStats = stats.shards[ shard ]
       
       var estDataPercent = Math.floor( shardStats.size / stats.size * 10000 ) / 100
       var estDocPercent = Math.floor( shardStats.count / stats.count * 10000 ) / 100
       
       print( " Shard " + shard + " contains " + estDataPercent + "% data, " + estDocPercent + "% docs in cluster, " +
              "avg obj size on shard : " + sh._dataFormat( stats.shards[ shard ].avgObjSize ) )
   }
   
   print( "\n" )
   
}


DBCollection.prototype.getSplitKeysForChunks = function( chunkSize ){
       
   var stats = this.stats()
   
   if( ! stats.sharded ){
       print( "Collection " + this + " is not sharded." )
       return
   }
   
   var config = this.getMongo().getDB("config")
   
   if( ! chunkSize ){
       chunkSize = config.settings.findOne({ _id : "chunksize" }).value
       print( "Chunk size not set, using default of " + chunkSize + "Mb" )
   }
   else{
       print( "Using chunk size of " + chunkSize + "Mb" )
   }
    
   var shardDocs = config.shards.find().toArray()
   
   var allSplitPoints = {}
   var numSplits = 0    
   
   for( var i = 0; i < shardDocs.length; i++ ){
       
       var shardDoc = shardDocs[i]
       var shard = shardDoc._id
       var host = shardDoc.host
       var sconn = new Mongo( host )
       
       var chunks = config.chunks.find({ _id : sh._collRE( this ), shard : shard }).toArray()
       
       print( "\nGetting split points for chunks on shard " + shard + " at " + host )
               
       var splitPoints = []
       
       for( var j = 0; j < chunks.length; j++ ){
           var chunk = chunks[j]
           var result = sconn.getDB("admin").runCommand({ splitVector : this + "", min : chunk.min, max : chunk.max, maxChunkSize : chunkSize })
           if( ! result.ok ){
               print( " Had trouble getting split keys for chunk " + sh._pchunk( chunk ) + " :\n" )
               printjson( result )
           }
           else{
               splitPoints = splitPoints.concat( result.splitKeys )
               
               if( result.splitKeys.length > 0 )
                   print( " Added " + result.splitKeys.length + " split points for chunk " + sh._pchunk( chunk ) )
           }
       }
       
       print( "Total splits for shard " + shard + " : " + splitPoints.length )
       
       numSplits += splitPoints.length
       allSplitPoints[ shard ] = splitPoints
       
   }
   
   // Get most recent migration
   var migration = config.changelog.find({ what : /^move.*/ }).sort({ time : -1 }).limit( 1 ).toArray()
   if( migration.length == 0 ) 
       print( "\nNo migrations found in changelog." )
   else {
       migration = migration[0]
       print( "\nMost recent migration activity was on " + migration.ns + " at " + migration.time )
   }
   
   var admin = this.getMongo().getDB("admin") 
   var coll = this
   var splitFunction = function(){
       
       // Turn off the balancer, just to be safe
       print( "Turning off balancer..." )
       config.settings.update({ _id : "balancer" }, { $set : { stopped : true } }, true )
       print( "Sleeping for 30s to allow balancers to detect change.  To be extra safe, check config.changelog" +
              " for recent migrations." )
       sleep( 30000 )
              
       for( shard in allSplitPoints ){
           for( var i = 0; i < allSplitPoints[ shard ].length; i++ ){
               var splitKey = allSplitPoints[ shard ][i]
               print( "Splitting at " + tojson( splitKey ) )
               printjson( admin.runCommand({ split : coll + "", middle : splitKey }) )
           }
       }
       
       print( "Turning the balancer back on." )
       config.settings.update({ _id : "balancer" }, { $set : { stopped : false } } )
       sleep( 1 )
   }
   
   splitFunction.getSplitPoints = function(){ return allSplitPoints; }
   
   print( "\nGenerated " + numSplits + " split keys, run output function to perform splits.\n" +
          " ex : \n" + 
          "  > var splitter = <collection>.getSplitKeysForChunks()\n" +
          "  > splitter() // Execute splits on cluster !\n" )
       
   return splitFunction
   
}

DBCollection.prototype.setSlaveOk = function( value ) {
    if( value == undefined ) value = true;
    this._slaveOk = value;
}

DBCollection.prototype.getSlaveOk = function() {
    if (this._slaveOk != undefined) return this._slaveOk;
    return this._db.getSlaveOk();
}

DBCollection.prototype.getQueryOptions = function() {
    var options = 0;
    if (this.getSlaveOk()) options |= 4;
    return options;
}

// db.js

if ( typeof DB == "undefined" ){                     
    DB = function( mongo , name ){
        this._mongo = mongo;
        this._name = name;
    }
}

DB.prototype.getMongo = function(){
    assert( this._mongo , "why no mongo!" );
    return this._mongo;
}

DB.prototype.getSiblingDB = function( name ){
    return this.getMongo().getDB( name );
}

DB.prototype.getSisterDB = DB.prototype.getSiblingDB;

DB.prototype.getName = function(){
    return this._name;
}

DB.prototype.stats = function(scale){
    return this.runCommand( { dbstats : 1 , scale : scale } );
}

DB.prototype.getCollection = function( name ){
    return new DBCollection( this._mongo , this , name , this._name + "." + name );
}

DB.prototype.commandHelp = function( name ){
    var c = {};
    c[name] = 1;
    c.help = true;
    var res = this.runCommand( c );
    if ( ! res.ok )
        throw res.errmsg;
    return res.help;
}

DB.prototype.runCommand = function( obj ){
    if ( typeof( obj ) == "string" ){
        var n = {};
        n[obj] = 1;
        obj = n;
    }
    return this.getCollection( "$cmd" ).findOne( obj );
}

DB.prototype._dbCommand = DB.prototype.runCommand;

DB.prototype.adminCommand = function( obj ){
    if ( this._name == "admin" )
        return this.runCommand( obj );
    return this.getSiblingDB( "admin" ).runCommand( obj );
}

DB.prototype._adminCommand = DB.prototype.adminCommand; // alias old name

DB.prototype.addUser = function( username , pass, readOnly, replicatedTo, timeout ){
    if ( pass == null || pass.length == 0 )
        throw "password can't be empty";

    readOnly = readOnly || false;
    var c = this.getCollection( "system.users" );
    
    var u = c.findOne( { user : username } ) || { user : username };
    u.readOnly = readOnly;
    u.pwd = hex_md5( username + ":mongo:" + pass );

    try {
        c.save( u );
    } catch (e) {
        // SyncClusterConnections call GLE automatically after every write and will throw an
        // exception if the insert failed.
        if ( tojson(e).indexOf( "login" ) >= 0 ){
            // TODO: this check is a hack
            print( "Creating user seems to have succeeded but threw an exception because we no " +
                   "longer have auth." );
        } else {
            throw "Could not insert into system.users: " + tojson(e);
        }
    }
    print( tojson( u ) );

    //
    // When saving users to replica sets, the shell user will want to know if the user hasn't
    // been fully replicated everywhere, since this will impact security.  By default, replicate to
    // majority of nodes with wtimeout 15 secs, though user can override
    //
    
    replicatedTo = replicatedTo != undefined && replicatedTo != null ? replicatedTo : "majority"
    
    // in mongod version 2.1.0-, this worked
    var le = {};
    try {        
        le = this.getLastErrorObj( replicatedTo, timeout || 30 * 1000 );
        // printjson( le )
    }
    catch (e) {
        errjson = tojson(e);
        if ( errjson.indexOf( "login" ) >= 0 || errjson.indexOf( "unauthorized" ) >= 0 ) {
            // TODO: this check is a hack
            print( "addUser succeeded, but cannot wait for replication since we no longer have auth" );
            return "";
        }
        print( "could not find getLastError object : " + tojson( e ) )
    }
    
    // We can't detect replica set shards via mongos, so we'll sometimes get this error
    // In this case though, we've already checked the local error before returning norepl, so
    // the user has been written and we're happy
    if( le.err == "norepl" ){
        return
    }        
    
    if ( le.err == "timeout" ){
        throw "timed out while waiting for user authentication to replicate - " +
              "database will not be fully secured until replication finishes"
    }
    
    if ( le.err )
        throw "couldn't add user: " + le.err
}

DB.prototype.logout = function(){
    return this.getMongo().logout(this.getName());
};

DB.prototype.removeUser = function( username ){
    this.getCollection( "system.users" ).remove( { user : username } );
}

DB.prototype.__pwHash = function( nonce, username, pass ) {
    return hex_md5( nonce + username + hex_md5( username + ":mongo:" + pass ) );
}

DB.prototype.auth = function( username , pass ){
    var result = 0;
    try {
        result = this.getMongo().auth(this.getName(), username, pass);
    }
    catch (e) {
        print(e);
        return 0;
    }
    return 1;
}

/**
  Create a new collection in the database.  Normally, collection creation is automatic.  You would
   use this function if you wish to specify special options on creation.

   If the collection already exists, no action occurs.
   
   <p>Options:</p>
   <ul>
   	<li>
     size: desired initial extent size for the collection.  Must be <= 1000000000.
           for fixed size (capped) collections, this size is the total/max size of the 
           collection.
    </li>
    <li>
     capped: if true, this is a capped collection (where old data rolls out).
    </li>
    <li> max: maximum number of objects if capped (optional).</li>
    </ul>

   <p>Example: </p>
   
   <code>db.createCollection("movies", { size: 10 * 1024 * 1024, capped:true } );</code>
 
 * @param {String} name Name of new collection to create 
 * @param {Object} options Object with options for call.  Options are listed above.
 * @return SOMETHING_FIXME
*/
DB.prototype.createCollection = function(name, opt) {
    var options = opt || {};
    var cmd = { create: name, capped: options.capped, size: options.size };
    if (options.max != undefined)
        cmd.max = options.max;
    if (options.autoIndexId != undefined)
        cmd.autoIndexId = options.autoIndexId;
    var res = this._dbCommand(cmd);
    return res;
}

/**
 * @deprecated use getProfilingStatus
 *  Returns the current profiling level of this database
 *  @return SOMETHING_FIXME or null on error
 */
DB.prototype.getProfilingLevel  = function() {
    var res = this._dbCommand( { profile: -1 } );
    return res ? res.was : null;
}

/**
 *  @return the current profiling status
 *  example { was : 0, slowms : 100 }
 *  @return SOMETHING_FIXME or null on error
 */
DB.prototype.getProfilingStatus  = function() {
    var res = this._dbCommand( { profile: -1 } );
    if ( ! res.ok )
        throw "profile command failed: " + tojson( res );
    delete res.ok
    return res;
}


/**
  Erase the entire database.  (!)

 * @return Object returned has member ok set to true if operation succeeds, false otherwise.
*/
DB.prototype.dropDatabase = function() {
    if ( arguments.length )
        throw "dropDatabase doesn't take arguments";
    return this._dbCommand( { dropDatabase: 1 } );
}

/**
 * Shuts down the database.  Must be run while using the admin database.
 * @param opts Options for shutdown. Possible options are:
 *   - force: (boolean) if the server should shut down, even if there is no
 *     up-to-date slave
 *   - timeoutSecs: (number) the server will continue checking over timeoutSecs
 *     if any other servers have caught up enough for it to shut down.
 */
DB.prototype.shutdownServer = function(opts) {
    if( "admin" != this._name ){
	return "shutdown command only works with the admin database; try 'use admin'";
    }

    cmd = {"shutdown" : 1};
    opts = opts || {};
    for (var o in opts) {
        cmd[o] = opts[o];
    }

    try {
        var res = this.runCommand(cmd);
	if( res )
	    throw "shutdownServer failed: " + res.errmsg;
	throw "shutdownServer failed";
    }
    catch ( e ){
        assert( tojson( e ).indexOf( "error doing query: failed" ) >= 0 , "unexpected error: " + tojson( e ) );
        print( "server should be down..." );
    }
}

/**
  Clone database on another server to here.
  <p>
  Generally, you should dropDatabase() first as otherwise the cloned information will MERGE 
  into whatever data is already present in this database.  (That is however a valid way to use 
  clone if you are trying to do something intentionally, such as union three non-overlapping
  databases into one.)
  <p>
  This is a low level administrative function will is not typically used.

 * @param {String} from Where to clone from (dbhostname[:port]).  May not be this database 
                   (self) as you cannot clone to yourself.
 * @return Object returned has member ok set to true if operation succeeds, false otherwise.
 * See also: db.copyDatabase()
*/
DB.prototype.cloneDatabase = function(from) { 
    assert( isString(from) && from.length );
    //this.resetIndexCache();
    return this._dbCommand( { clone: from } );
}


/**
 Clone collection on another server to here.
 <p>
 Generally, you should drop() first as otherwise the cloned information will MERGE 
 into whatever data is already present in this collection.  (That is however a valid way to use 
 clone if you are trying to do something intentionally, such as union three non-overlapping
 collections into one.)
 <p>
 This is a low level administrative function is not typically used.
 
 * @param {String} from mongod instance from which to clnoe (dbhostname:port).  May
 not be this mongod instance, as clone from self is not allowed.
 * @param {String} collection name of collection to clone.
 * @param {Object} query query specifying which elements of collection are to be cloned.
 * @return Object returned has member ok set to true if operation succeeds, false otherwise.
 * See also: db.cloneDatabase()
 */
DB.prototype.cloneCollection = function(from, collection, query) { 
    assert( isString(from) && from.length );
    assert( isString(collection) && collection.length );
    collection = this._name + "." + collection;
    query = query || {};
    //this.resetIndexCache();
    return this._dbCommand( { cloneCollection:collection, from:from, query:query } );
}


/**
  Copy database from one server or name to another server or name.

  Generally, you should dropDatabase() first as otherwise the copied information will MERGE 
  into whatever data is already present in this database (and you will get duplicate objects 
  in collections potentially.)

  For security reasons this function only works when executed on the "admin" db.  However, 
  if you have access to said db, you can copy any database from one place to another.

  This method provides a way to "rename" a database by copying it to a new db name and 
  location.  Additionally, it effectively provides a repair facility.

  * @param {String} fromdb database name from which to copy.
  * @param {String} todb database name to copy to.
  * @param {String} fromhost hostname of the database (and optionally, ":port") from which to 
                    copy the data.  default if unspecified is to copy from self.
  * @return Object returned has member ok set to true if operation succeeds, false otherwise.
  * See also: db.clone()
*/
DB.prototype.copyDatabase = function(fromdb, todb, fromhost, username, password) { 
    assert( isString(fromdb) && fromdb.length );
    assert( isString(todb) && todb.length );
    fromhost = fromhost || "";
    if ( username && password ) {
        var n = this._adminCommand( { copydbgetnonce : 1, fromhost:fromhost } );
        return this._adminCommand( { copydb:1, fromhost:fromhost, fromdb:fromdb, todb:todb, username:username, nonce:n.nonce, key:this.__pwHash( n.nonce, username, password ) } );
    } else {
        return this._adminCommand( { copydb:1, fromhost:fromhost, fromdb:fromdb, todb:todb } );
    }
}

/**
  Repair database.
 
 * @return Object returned has member ok set to true if operation succeeds, false otherwise.
*/
DB.prototype.repairDatabase = function() {
    return this._dbCommand( { repairDatabase: 1 } );
}


DB.prototype.help = function() {
    print("DB methods:");
    print("\tdb.addUser(username, password[, readOnly=false])");
    print("\tdb.adminCommand(nameOrDocument) - switches to 'admin' db, and runs command [ just calls db.runCommand(...) ]");
    print("\tdb.auth(username, password)");
    print("\tdb.cloneDatabase(fromhost)");
    print("\tdb.commandHelp(name) returns the help for the command");
    print("\tdb.copyDatabase(fromdb, todb, fromhost)");
    print("\tdb.createCollection(name, { size : ..., capped : ..., max : ... } )");
    print("\tdb.currentOp() displays currently executing operations in the db");
    print("\tdb.dropDatabase()");
    print("\tdb.eval(func, args) run code server-side");
    print("\tdb.fsyncLock() flush data to disk and lock server for backups");
    print("\tdb.fsyncUnlock() unlocks server following a db.fsyncLock()");
    print("\tdb.getCollection(cname) same as db['cname'] or db.cname");
    print("\tdb.getCollectionNames()");
    print("\tdb.getLastError() - just returns the err msg string");
    print("\tdb.getLastErrorObj() - return full status object");
    print("\tdb.getMongo() get the server connection object");
    print("\tdb.getMongo().setSlaveOk() allow queries on a replication slave server");
    print("\tdb.getName()");
    print("\tdb.getPrevError()");
    print("\tdb.getProfilingLevel() - deprecated");
    print("\tdb.getProfilingStatus() - returns if profiling is on and slow threshold");
    print("\tdb.getReplicationInfo()");
    print("\tdb.getSiblingDB(name) get the db at the same server as this one");
    print("\tdb.hostInfo() get details about the server's host"); 
    print("\tdb.isMaster() check replica primary status");
    print("\tdb.killOp(opid) kills the current operation in the db");
    print("\tdb.listCommands() lists all the db commands");
    print("\tdb.loadServerScripts() loads all the scripts in db.system.js");
    print("\tdb.logout()");
    print("\tdb.printCollectionStats()");
    print("\tdb.printReplicationInfo()");
    print("\tdb.printShardingStatus()");
    print("\tdb.printSlaveReplicationInfo()");
    print("\tdb.removeUser(username)");
    print("\tdb.repairDatabase()");
    print("\tdb.resetError()");
    print("\tdb.runCommand(cmdObj) run a database command.  if cmdObj is a string, turns it into { cmdObj : 1 }");
    print("\tdb.serverStatus()");
    print("\tdb.setProfilingLevel(level,<slowms>) 0=off 1=slow 2=all");
    print("\tdb.setVerboseShell(flag) display extra information in shell output");
    print("\tdb.shutdownServer()");
    print("\tdb.stats()");
    print("\tdb.version() current version of the server");

    return __magicNoPrint;
}

DB.prototype.printCollectionStats = function(){
    var mydb = this;
    this.getCollectionNames().forEach(
        function(z){
            print( z );
            printjson( mydb.getCollection(z).stats() );
            print( "---" );
        }
    );
}

/**
 * <p> Set profiling level for your db.  Profiling gathers stats on query performance. </p>
 * 
 * <p>Default is off, and resets to off on a database restart -- so if you want it on,
 *    turn it on periodically. </p>
 *  
 *  <p>Levels :</p>
 *   <ul>
 *    <li>0=off</li>
 *    <li>1=log very slow operations; optional argument slowms specifies slowness threshold</li>
 *    <li>2=log all</li>
 *  @param {String} level Desired level of profiling
 *  @param {String} slowms For slow logging, query duration that counts as slow (default 100ms)
 *  @return SOMETHING_FIXME or null on error
 */
DB.prototype.setProfilingLevel = function(level,slowms) {
    
    if (level < 0 || level > 2) { 
        throw { dbSetProfilingException : "input level " + level + " is out of range [0..2]" };        
    }

    var cmd = { profile: level };
    if ( slowms )
        cmd["slowms"] = slowms;
    return this._dbCommand( cmd );
}

DB.prototype._initExtraInfo = function() {
    if ( typeof _verboseShell === 'undefined' || !_verboseShell ) return;
    this.startTime = new Date().getTime();
}

DB.prototype._getExtraInfo = function(action) {
    if ( typeof _verboseShell === 'undefined' || !_verboseShell ) {
        __callLastError = true;
        return;
    }

    // explicit w:1 so that replset getLastErrorDefaults aren't used here which would be bad.
    var res = this.getLastErrorCmd(1); 
    if (res) {
        if (res.err != undefined && res.err != null) {
            // error occured, display it
            print(res.err);
            return;
        }

        var info = action + " ";  
        // hack for inserted because res.n is 0
        info += action != "Inserted" ? res.n : 1;
        if (res.n > 0 && res.updatedExisting != undefined) info += " " + (res.updatedExisting ? "existing" : "new")  
        info += " record(s)";  
        var time = new Date().getTime() - this.startTime;  
        info += " in " + time + "ms";
        print(info);
    }
} 

/**
 *  <p> Evaluate a js expression at the database server.</p>
 * 
 * <p>Useful if you need to touch a lot of data lightly; in such a scenario
 *  the network transfer of the data could be a bottleneck.  A good example
 *  is "select count(*)" -- can be done server side via this mechanism.
 * </p>
 *
 * <p>
 * If the eval fails, an exception is thrown of the form:
 * </p>
 * <code>{ dbEvalException: { retval: functionReturnValue, ok: num [, errno: num] [, errmsg: str] } }</code>
 * 
 * <p>Example: </p>
 * <code>print( "mycount: " + db.eval( function(){db.mycoll.find({},{_id:ObjId()}).length();} );</code>
 *
 * @param {Function} jsfunction Javascript function to run on server.  Note this it not a closure, but rather just "code".
 * @return result of your function, or null if error
 * 
 */
DB.prototype.eval = function(jsfunction) {
    var cmd = { $eval : jsfunction };
    if ( arguments.length > 1 ) {
	cmd.args = argumentsToArray( arguments ).slice(1);
    }
    
    var res = this._dbCommand( cmd );
    
    if (!res.ok)
    	throw tojson( res );
    
    return res.retval;
}

DB.prototype.dbEval = DB.prototype.eval;


/**
 * 
 *  <p>
 *   Similar to SQL group by.  For example: </p>
 *
 *  <code>select a,b,sum(c) csum from coll where active=1 group by a,b</code>
 *
 *  <p>
 *    corresponds to the following in 10gen:
 *  </p>
 * 
 *  <code>
     db.group(
       {
         ns: "coll",
         key: { a:true, b:true },
	 // keyf: ...,
	 cond: { active:1 },
	 reduce: function(obj,prev) { prev.csum += obj.c; } ,
	 initial: { csum: 0 }
	 });
	 </code>
 *
 * 
 * <p>
 *  An array of grouped items is returned.  The array must fit in RAM, thus this function is not
 * suitable when the return set is extremely large.
 * </p>
 * <p>
 * To order the grouped data, simply sort it client side upon return.
 * <p>
   Defaults
     cond may be null if you want to run against all rows in the collection
     keyf is a function which takes an object and returns the desired key.  set either key or keyf (not both).
 * </p>
*/
DB.prototype.groupeval = function(parmsObj) {
	
    var groupFunction = function() {
	var parms = args[0];
    	var c = db[parms.ns].find(parms.cond||{});
    	var map = new Map();
        var pks = parms.key ? Object.keySet( parms.key ) : null;
        var pkl = pks ? pks.length : 0;
        var key = {};
        
    	while( c.hasNext() ) {
	    var obj = c.next();
	    if ( pks ) {
	    	for( var i=0; i<pkl; i++ ){
                    var k = pks[i];
		    key[k] = obj[k];
                }
	    }
	    else {
	    	key = parms.$keyf(obj);
	    }

	    var aggObj = map.get(key);
	    if( aggObj == null ) {
		var newObj = Object.extend({}, key); // clone
	    	aggObj = Object.extend(newObj, parms.initial)
                map.put( key , aggObj );
	    }
	    parms.$reduce(obj, aggObj);
	}
        
	return map.values();
    }
    
    return this.eval(groupFunction, this._groupFixParms( parmsObj ));
}

DB.prototype.groupcmd = function( parmsObj ){
    var ret = this.runCommand( { "group" : this._groupFixParms( parmsObj ) } );
    if ( ! ret.ok ){
        throw "group command failed: " + tojson( ret );
    }
    return ret.retval;
}

DB.prototype.group = DB.prototype.groupcmd;

DB.prototype._groupFixParms = function( parmsObj ){
    var parms = Object.extend({}, parmsObj);
    
    if( parms.reduce ) {
	parms.$reduce = parms.reduce; // must have $ to pass to db
	delete parms.reduce;
    }
    
    if( parms.keyf ) {
	parms.$keyf = parms.keyf;
	delete parms.keyf;
    }
    
    return parms;
}

DB.prototype.resetError = function(){
    return this.runCommand( { reseterror : 1 } );
}

DB.prototype.forceError = function(){
    return this.runCommand( { forceerror : 1 } );
}

DB.prototype.getLastError = function( w , wtimeout ){
    var res = this.getLastErrorObj( w , wtimeout );
    if ( ! res.ok )
        throw "getlasterror failed: " + tojson( res );
    return res.err;
}
DB.prototype.getLastErrorObj = function( w , wtimeout ){
    var cmd = { getlasterror : 1 };
    if ( w ){
        cmd.w = w;
        if ( wtimeout )
            cmd.wtimeout = wtimeout;
    }
    var res = this.runCommand( cmd );

    if ( ! res.ok )
        throw "getlasterror failed: " + tojson( res );
    return res;
}
DB.prototype.getLastErrorCmd = DB.prototype.getLastErrorObj;


/* Return the last error which has occurred, even if not the very last error.

   Returns: 
    { err : <error message>, nPrev : <how_many_ops_back_occurred>, ok : 1 }

   result.err will be null if no error has occurred.
 */
DB.prototype.getPrevError = function(){
    return this.runCommand( { getpreverror : 1 } );
}

DB.prototype.getCollectionNames = function(){
    var all = [];

    var nsLength = this._name.length + 1;
    
    var c = this.getCollection( "system.namespaces" ).find();
    while ( c.hasNext() ){
        var name = c.next().name;
        
        if ( name.indexOf( "$" ) >= 0 && name.indexOf( ".oplog.$" ) < 0 )
            continue;
        
        all.push( name.substring( nsLength ) );
    }
    
    return all.sort();
}

DB.prototype.tojson = function(){
    return this._name;
}

DB.prototype.toString = function(){
    return this._name;
}

DB.prototype.isMaster = function () { return this.runCommand("isMaster"); }

DB.prototype.currentOp = function( arg ){
    var q = {}
    if ( arg ) {
        if ( typeof( arg ) == "object" )
            Object.extend( q , arg );
        else if ( arg )
            q["$all"] = true;
    }
    return this.$cmd.sys.inprog.findOne( q );
}
DB.prototype.currentOP = DB.prototype.currentOp;

DB.prototype.killOp = function(op) {
    if( !op ) 
        throw "no opNum to kill specified";
    return this.$cmd.sys.killop.findOne({'op':op});
}
DB.prototype.killOP = DB.prototype.killOp;

DB.tsToSeconds = function(x){
    if ( x.t && x.i )
        return x.t / 1000;
    return x / 4294967296; // low 32 bits are ordinal #s within a second
}

/** 
  Get a replication log information summary.
  <p>
  This command is for the database/cloud administer and not applicable to most databases.
  It is only used with the local database.  One might invoke from the JS shell:
  <pre>
       use local
       db.getReplicationInfo();
  </pre>
  It is assumed that this database is a replication master -- the information returned is 
  about the operation log stored at local.oplog.$main on the replication master.  (It also 
  works on a machine in a replica pair: for replica pairs, both machines are "masters" from 
  an internal database perspective.
  <p>
  * @return Object timeSpan: time span of the oplog from start to end  if slave is more out 
  *                          of date than that, it can't recover without a complete resync
*/
DB.prototype.getReplicationInfo = function() { 
    var db = this.getSiblingDB("local");

    var result = { };
    var oplog;
    if (db.system.namespaces.findOne({name:"local.oplog.rs"}) != null) {
        oplog = 'oplog.rs';
    }
    else if (db.system.namespaces.findOne({name:"local.oplog.$main"}) != null) {
        oplog = 'oplog.$main';
    }
    else {
        result.errmsg = "neither master/slave nor replica set replication detected";
        return result;
    }
    
    var ol_entry = db.system.namespaces.findOne({name:"local."+oplog});
    if( ol_entry && ol_entry.options ) {
	result.logSizeMB = ol_entry.options.size / ( 1024 * 1024 );
    } else {
        result.errmsg  = "local."+oplog+", or its options, not found in system.namespaces collection";
        return result;
    }
    ol = db.getCollection(oplog);

    result.usedMB = ol.stats().size / ( 1024 * 1024 );
    result.usedMB = Math.ceil( result.usedMB * 100 ) / 100;
    
    var firstc = ol.find().sort({$natural:1}).limit(1);
    var lastc = ol.find().sort({$natural:-1}).limit(1);
    if( !firstc.hasNext() || !lastc.hasNext() ) { 
	result.errmsg = "objects not found in local.oplog.$main -- is this a new and empty db instance?";
	result.oplogMainRowCount = ol.count();
	return result;
    }

    var first = firstc.next();
    var last = lastc.next();
    {
	var tfirst = first.ts;
	var tlast = last.ts;
        
	if( tfirst && tlast ) { 
	    tfirst = DB.tsToSeconds( tfirst ); 
	    tlast = DB.tsToSeconds( tlast );
	    result.timeDiff = tlast - tfirst;
	    result.timeDiffHours = Math.round(result.timeDiff / 36)/100;
	    result.tFirst = (new Date(tfirst*1000)).toString();
	    result.tLast  = (new Date(tlast*1000)).toString();
	    result.now = Date();
	}
	else { 
	    result.errmsg = "ts element not found in oplog objects";
	}
    }

    return result;
};

DB.prototype.printReplicationInfo = function() {
    var result = this.getReplicationInfo();
    if( result.errmsg ) {
        if (!this.isMaster().ismaster) {
            print("this is a slave, printing slave replication info.");
            this.printSlaveReplicationInfo();
            return;
        }
	print(tojson(result));
	return;
    }
    print("configured oplog size:   " + result.logSizeMB + "MB");
    print("log length start to end: " + result.timeDiff + "secs (" + result.timeDiffHours + "hrs)");
    print("oplog first event time:  " + result.tFirst);
    print("oplog last event time:   " + result.tLast);
    print("now:                     " + result.now);
}

DB.prototype.printSlaveReplicationInfo = function() {
    function getReplLag(st) {
        var now = new Date();
        print("\t syncedTo: " + st.toString() );
        var ago = (now-st)/1000;
        var hrs = Math.round(ago/36)/100;
        print("\t\t = " + Math.round(ago) + " secs ago (" + hrs + "hrs)");
    };
    
    function g(x) {
        assert( x , "how could this be null (printSlaveReplicationInfo gx)" )
        print("source:   " + x.host);
        if ( x.syncedTo ){
            var st = new Date( DB.tsToSeconds( x.syncedTo ) * 1000 );
            getReplLag(st);
        }
        else {
            print( "\t doing initial sync" );
        }
    };

    function r(x) {
        assert( x , "how could this be null (printSlaveReplicationInfo rx)" );
        if ( x.state == 1 ) {
            return;
        }
        
        print("source:   " + x.name);
        if ( x.optime ) {
            getReplLag(x.optimeDate);
        }
        else {
            print( "\t no replication info, yet.  State: " + x.stateStr );
        }
    };
    
    var L = this.getSiblingDB("local");

    if (L.system.replset.count() != 0) {
        var status = this.adminCommand({'replSetGetStatus' : 1});
        status.members.forEach(r);
    }
    else if( L.sources.count() != 0 ) {
        L.sources.find().forEach(g);
    }
    else {
        print("local.sources is empty; is this db a --slave?");
        return;
    }
}

DB.prototype.serverBuildInfo = function(){
    return this._adminCommand( "buildinfo" );
}

DB.prototype.serverStatus = function(){
    return this._adminCommand( "serverStatus" );
}

DB.prototype.hostInfo = function(){
    return this._adminCommand( "hostInfo" );
}

DB.prototype.serverCmdLineOpts = function(){
    return this._adminCommand( "getCmdLineOpts" );
}

DB.prototype.version = function(){
    return this.serverBuildInfo().version;
}

DB.prototype.serverBits = function(){
    return this.serverBuildInfo().bits;
}

DB.prototype.listCommands = function(){
    var x = this.runCommand( "listCommands" );
    for ( var name in x.commands ){
        var c = x.commands[name];

        var s = name + ": ";
        
        switch ( c.lockType ){
        case -1: s += "read-lock"; break;
        case  0: s += "no-lock"; break;
        case  1: s += "write-lock"; break;
        default: s += c.lockType;
        }
        
        if (c.adminOnly) s += " adminOnly ";
        if (c.adminOnly) s += " slaveOk ";

        s += "\n  ";
        s += c.help.replace(/\n/g, '\n  ');
        s += "\n";
        
        print( s );
    }
}

DB.prototype.printShardingStatus = function( verbose ){
    printShardingStatus( this.getSiblingDB( "config" ) , verbose );
}

DB.prototype.fsyncLock = function() {
    return this.adminCommand({fsync:1, lock:true});
}

DB.prototype.fsyncUnlock = function() {
    return this.getSiblingDB("admin").$cmd.sys.unlock.findOne()
}

DB.autocomplete = function(obj){
    var colls = obj.getCollectionNames();
    var ret=[];
    for (var i=0; i<colls.length; i++){
        if (colls[i].match(/^[a-zA-Z0-9_.\$]+$/))
            ret.push(colls[i]);
    }
    return ret;
}

DB.prototype.setSlaveOk = function( value ) {
    if( value == undefined ) value = true;
    this._slaveOk = value;
}

DB.prototype.getSlaveOk = function() {
    if (this._slaveOk != undefined) return this._slaveOk;
    return this._mongo.getSlaveOk();
}

/* Loads any scripts contained in system.js into the client shell.
*/
DB.prototype.loadServerScripts = function(){
    this.system.js.find().forEach(function(u){eval(u._id + " = " + u.value);});
}
// mr.js

MR = {};

MR.init = function(){
    $max = 0;
    $arr = [];
    emit = MR.emit;
    $numEmits = 0;
    $numReduces = 0;
    $numReducesToDB = 0;
    gc(); // this is just so that keep memory size sane
}

MR.cleanup = function(){
    MR.init();
    gc();
}

MR.emit = function(k,v){
    $numEmits++;
    var num = nativeHelper.apply( get_num_ , [ k ] );
    var data = $arr[num];
    if ( ! data ){
        data = { key : k , values : new Array(1000) , count : 0 };
        $arr[num] = data;
    }
    data.values[data.count++] = v;
    $max = Math.max( $max , data.count );
}

MR.doReduce = function( useDB ){
    $numReduces++;
    if ( useDB )
        $numReducesToDB++;
    $max = 0;
    for ( var i=0; i<$arr.length; i++){
        var data = $arr[i];
        if ( ! data ) 
            continue;
        
        if ( useDB ){
            var x = tempcoll.findOne( { _id : data.key } );
            if ( x ){
                data.values[data.count++] = x.value;
            }
        }

        var r = $reduce( data.key , data.values.slice( 0 , data.count ) );
        if ( r && r.length && r[0] ){ 
            data.values = r; 
            data.count = r.length;
        }
        else{ 
            data.values[0] = r;
            data.count = 1;
        }
        
        $max = Math.max( $max , data.count ); 
        
        if ( useDB ){
            if ( data.count == 1 ){
                tempcoll.save( { _id : data.key , value : data.values[0] } );
            }
            else {
                tempcoll.save( { _id : data.key , value : data.values.slice( 0 , data.count ) } );
            }
        }
    }
}

MR.check = function(){                        
    if ( $max < 2000 && $arr.length < 1000 ){ 
        return 0; 
    }
    MR.doReduce();
    if ( $max < 2000 && $arr.length < 1000 ){ 
        return 1;
    }
    MR.doReduce( true );
    $arr = []; 
    $max = 0; 
    reset_num();
    gc();
    return 2;
}

MR.finalize = function(){
    tempcoll.find().forEach( 
        function(z){
            z.value = $finalize( z._id , z.value );
            tempcoll.save( z );
        }
    );
}
// query.js

if ( typeof DBQuery == "undefined" ){
    DBQuery = function( mongo , db , collection , ns , query , fields , limit , skip , batchSize , options ){
        
        this._mongo = mongo; // 0
        this._db = db; // 1
        this._collection = collection; // 2
        this._ns = ns; // 3
        
        this._query = query || {}; // 4
        this._fields = fields; // 5
        this._limit = limit || 0; // 6
        this._skip = skip || 0; // 7
        this._batchSize = batchSize || 0;
        this._options = options || 0;

        this._cursor = null;
        this._numReturned = 0;
        this._special = false;
        this._prettyShell = false;
    }
    //print( "DBQuery probably won't have array access " );
}

DBQuery.prototype.help = function () {
    print("find() modifiers")
    print("\t.sort( {...} )")
    print("\t.limit( n )")
    print("\t.skip( n )")
    print("\t.count() - total # of objects matching query, ignores skip,limit")
    print("\t.size() - total # of objects cursor would return, honors skip,limit")
    print("\t.explain([verbose])")
    print("\t.hint(...)")
    print("\t.addOption(n) - adds op_query options -- see wire protocol")
    print("\t._addSpecial(name, value) - http://dochub.mongodb.org/core/advancedqueries#AdvancedQueries-Metaqueryoperators")
    print("\t.batchSize(n) - sets the number of docs to return per getMore")
    print("\t.showDiskLoc() - adds a $diskLoc field to each returned object")
    print("\t.min(idxDoc)")
    print("\t.max(idxDoc)")
    
    print("\nCursor methods");
    print("\t.toArray() - iterates through docs and returns an array of the results")
    print("\t.forEach( func )")
    print("\t.map( func )")
    print("\t.hasNext()")
    print("\t.next()")
    print("\t.objsLeftInBatch() - returns count of docs left in current batch (when exhausted, a new getMore will be issued)")
    print("\t.count(applySkipLimit) - runs command at server")    
    print("\t.itcount() - iterates through documents and counts them")
}

DBQuery.prototype.clone = function(){
    var q =  new DBQuery( this._mongo , this._db , this._collection , this._ns , 
        this._query , this._fields , 
        this._limit , this._skip , this._batchSize , this._options );
    q._special = this._special;
    return q;
}

DBQuery.prototype._ensureSpecial = function(){
    if ( this._special )
        return;
    
    var n = { query : this._query };
    this._query = n;
    this._special = true;
}

DBQuery.prototype._checkModify = function(){
    if ( this._cursor )
        throw "query already executed";
}

DBQuery.prototype._exec = function(){
    if ( ! this._cursor ){
        assert.eq( 0 , this._numReturned );
        this._cursor = this._mongo.find( this._ns , this._query , this._fields , this._limit , this._skip , this._batchSize , this._options );
        this._cursorSeen = 0;
    }
    return this._cursor;
}

DBQuery.prototype.limit = function( limit ){
    this._checkModify();
    this._limit = limit;
    return this;
}

DBQuery.prototype.batchSize = function( batchSize ){
    this._checkModify();
    this._batchSize = batchSize;
    return this;
}


DBQuery.prototype.addOption = function( option ){
    this._options |= option;
    return this;
}

DBQuery.prototype.skip = function( skip ){
    this._checkModify();
    this._skip = skip;
    return this;
}

DBQuery.prototype.hasNext = function(){
    this._exec();

    if ( this._limit > 0 && this._cursorSeen >= this._limit )
        return false;
    var o = this._cursor.hasNext();
    return o;
}

DBQuery.prototype.next = function(){
    this._exec();
    
    var o = this._cursor.hasNext();
    if ( o )
        this._cursorSeen++;
    else
        throw "error hasNext: " + o;
    
    var ret = this._cursor.next();
    if ( ret.$err && this._numReturned == 0 && ! this.hasNext() )
        throw "error: " + tojson( ret );

    this._numReturned++;
    return ret;
}

DBQuery.prototype.objsLeftInBatch = function(){
    this._exec();

    var ret = this._cursor.objsLeftInBatch();
    if ( ret.$err )
        throw "error: " + tojson( ret );

    return ret;
}

DBQuery.prototype.readOnly = function(){
    this._exec();
    this._cursor.readOnly();
    return this;
}

DBQuery.prototype.toArray = function(){
    if ( this._arr )
        return this._arr;
    
    var a = [];
    while ( this.hasNext() )
        a.push( this.next() );
    this._arr = a;
    return a;
}

DBQuery.prototype.count = function( applySkipLimit ){
    var cmd = { count: this._collection.getName() };
    if ( this._query ){
        if ( this._special )
            cmd.query = this._query.query;
        else 
            cmd.query = this._query;
    }
    cmd.fields = this._fields || {};

    if ( applySkipLimit ){
        if ( this._limit )
            cmd.limit = this._limit;
        if ( this._skip )
            cmd.skip = this._skip;
    }
    
    var res = this._db.runCommand( cmd );
    if( res && res.n != null ) return res.n;
    throw "count failed: " + tojson( res );
}

DBQuery.prototype.size = function(){
    return this.count( true );
}

DBQuery.prototype.countReturn = function(){
    var c = this.count();

    if ( this._skip )
        c = c - this._skip;

    if ( this._limit > 0 && this._limit < c )
        return this._limit;
    
    return c;
}

/**
* iterative count - only for testing
*/
DBQuery.prototype.itcount = function(){
    var num = 0;
    while ( this.hasNext() ){
        num++;
        this.next();
    }
    return num;
}

DBQuery.prototype.length = function(){
    return this.toArray().length;
}

DBQuery.prototype._addSpecial = function( name , value ){
    this._ensureSpecial();
    this._query[name] = value;
    return this;
}

DBQuery.prototype.sort = function( sortBy ){
    return this._addSpecial( "orderby" , sortBy );
}

DBQuery.prototype.hint = function( hint ){
    return this._addSpecial( "$hint" , hint );
}

DBQuery.prototype.min = function( min ) {
    return this._addSpecial( "$min" , min );
}

DBQuery.prototype.max = function( max ) {
    return this._addSpecial( "$max" , max );
}

DBQuery.prototype.showDiskLoc = function() {
    return this._addSpecial( "$showDiskLoc" , true);
}

/**
 * Sets the read preference for this cursor.
 * 
 * @param mode {string} read prefrence mode to use.
 * @param tagSet {Array.<Object>} optional. The list of tags to use, order matters.
 * 
 * @return this cursor
 */
DBQuery.prototype.readPref = function( mode, tagSet ) {
    var readPrefObj = {
        mode: mode
    };

    if ( tagSet ){
        readPrefObj.tags = tagSet;
    }

    return this._addSpecial( "$readPreference", readPrefObj );
};

DBQuery.prototype.forEach = function( func ){
    while ( this.hasNext() )
        func( this.next() );
}

DBQuery.prototype.map = function( func ){
    var a = [];
    while ( this.hasNext() )
        a.push( func( this.next() ) );
    return a;
}

DBQuery.prototype.arrayAccess = function( idx ){
    return this.toArray()[idx];
}
DBQuery.prototype.comment = function (comment) {
    var n = this.clone();
    n._ensureSpecial();
    n._addSpecial("$comment", comment);
    return this.next();
}

DBQuery.prototype.explain = function (verbose) {
    /* verbose=true --> include allPlans, oldPlan fields */
    var n = this.clone();
    n._ensureSpecial();
    n._query.$explain = true;
    n._limit = Math.abs(n._limit) * -1;
    var e = n.next();

    function cleanup(obj){
        if (typeof(obj) != 'object'){
            return;
        }

        delete obj.allPlans;
        delete obj.oldPlan;

        if (typeof(obj.length) == 'number'){
            for (var i=0; i < obj.length; i++){
                cleanup(obj[i]);
            }
        }

        if (obj.shards){
            for (var key in obj.shards){
                cleanup(obj.shards[key]);
            }
        }

        if (obj.clauses){
            cleanup(obj.clauses);
        }
    }

    if (!verbose)
        cleanup(e);

    return e;
}

DBQuery.prototype.snapshot = function(){
    this._ensureSpecial();
    this._query.$snapshot = true;
    return this;
}

DBQuery.prototype.pretty = function(){
    this._prettyShell = true;
    return this;
}

DBQuery.prototype.shellPrint = function(){
    try {
        var start = new Date().getTime();
        var n = 0;
        while ( this.hasNext() && n < DBQuery.shellBatchSize ){
            var s = this._prettyShell ? tojson( this.next() ) : tojson( this.next() , "" , true );
            print( s );
            n++;
        }
        if (typeof _verboseShell !== 'undefined' && _verboseShell) {
            var time = new Date().getTime() - start;
            print("Fetched " + n + " record(s) in " + time + "ms");
        }
         if ( this.hasNext() ){
            print( "Type \"it\" for more" );
            ___it___  = this;
        }
        else {
            ___it___  = null;
        }
   }
    catch ( e ){
        print( e );
    }
    
}

DBQuery.prototype.toString = function(){
    return "DBQuery: " + this._ns + " -> " + tojson( this.query );
}

DBQuery.shellBatchSize = 20;

/**
 * Query option flag bit constants.
 * @see http://dochub.mongodb.org/core/mongowireprotocol#MongoWireProtocol-OPQUERY
 */
DBQuery.Option = {
    tailable: 0x2,
    slaveOk: 0x4,
    oplogReplay: 0x8,
    noTimeout: 0x10,
    awaitData: 0x20,
    exhaust: 0x40,
    partial: 0x80
};

ReplSetBridge = function(rst, from, to) {
    var n = rst.nodes.length;

    var startPort = rst.startPort+n;
    this.port = (startPort+(from*n+to));
    this.host = rst.host+":"+this.port;

    this.dest = rst.host+":"+rst.ports[to];
    this.start();
};

ReplSetBridge.prototype.start = function() {
    var args = ["mongobridge", "--port", this.port, "--dest", this.dest];
    print("ReplSetBridge starting: "+tojson(args));
    this.bridge = startMongoProgram.apply( null , args );
    print("ReplSetBridge started " + this.bridge);
};

ReplSetBridge.prototype.stop = function() {
    print("ReplSetBridge stopping: " + this.port);
    stopMongod(this.port, 9);
};

ReplSetBridge.prototype.toString = function() {
    return this.host+" -> "+this.dest;
};
/**
 * Sets up a replica set. To make the set running, call {@link #startSet},
 * followed by {@link #initiate} (and optionally,
 * {@link #awaitSecondaryNodes} to block till the  set is fully operational).
 * Note that some of the replica start up parameters are not passed here,
 * but to the #startSet method.
 * 
 * @param {Object} opts
 * 
 *   {
 *     name {string}: name of this replica set. Default: 'testReplSet'
 *     host {string}: name of the host machine. Hostname will be used
 *        if not specified.
 *     useHostName {boolean}: if true, use hostname of machine,
 *        otherwise use localhost
 *     nodes {number|Object|Array.<Object>}: number of replicas. Default: 0.
 *        Can also be an Object (or Array).
 *        Format for Object:
 *          {
 *            <any string>: replica member option Object. @see MongoRunner.runMongod
 *            <any string2>: and so on...
 *          }
 * 
 *        Format for Array:
 *           An array of replica member option Object. @see MongoRunner.runMongod
 * 
 *        Note: For both formats, a special boolean property 'arbiter' can be
 *          specified to denote a member is an arbiter.
 * 
 *     oplogSize {number}: Default: 40
 *     useSeedList {boolean}: Use the connection string format of this set
 *        as the replica set name (overrides the name property). Default: false
 *     bridged {boolean}: Whether to set a mongobridge between replicas.
 *        Default: false
 *     keyFile {string}
 *     shardSvr {boolean}: Default: false
 *     startPort {number}: port offset to be used for each replica. Default: 31000
 *   }
 * 
 * Member variables:
 * numNodes {number} - number of nodes
 * nodes {Array.<Mongo>} - connection to replica set members
 */
ReplSetTest = function( opts ){
    this.name  = opts.name || "testReplSet";
    this.useHostName = opts.useHostName == undefined ? true : opts.useHostName;
    this.host  = this.useHostName ? (opts.host || getHostName()) : 'localhost';
    this.numNodes = opts.nodes || 0;
    this.oplogSize = opts.oplogSize || 40;
    this.useSeedList = opts.useSeedList || false;
    this.bridged = opts.bridged || false;
    this.ports = [];
    this.keyFile = opts.keyFile
    this.shardSvr = opts.shardSvr || false;

    this.startPort = opts.startPort || 31000;

    this.nodeOptions = {}    
    if( isObject( this.numNodes ) ){
        var len = 0
        for( var i in this.numNodes ){
            var options = this.nodeOptions[ "n" + len ] = this.numNodes[i]
            if( i.startsWith( "a" ) ) options.arbiter = true
            len++
        }
        this.numNodes = len
    }
    else if( Array.isArray( this.numNodes ) ){
        for( var i = 0; i < this.numNodes.length; i++ )
            this.nodeOptions[ "n" + i ] = this.numNodes[i]
        this.numNodes = this.numNodes.length
    }
    
    if(this.bridged) {
        this.bridgePorts = [];

        var allPorts = allocatePorts( this.numNodes * 2 , this.startPort );
        for(var i=0; i < this.numNodes; i++) {
            this.ports[i] = allPorts[i*2];
            this.bridgePorts[i] = allPorts[i*2 + 1];
        }

        this.initBridges();
    }
    else {
        this.ports = allocatePorts( this.numNodes , this.startPort );
    }

    this.nodes = []
    this.initLiveNodes()
    
    Object.extend( this, ReplSetTest.Health )
    Object.extend( this, ReplSetTest.State )
    
}

ReplSetTest.prototype.initBridges = function() {
    for(var i=0; i<this.ports.length; i++) {
        startMongoProgram( "mongobridge", "--port", this.bridgePorts[i], "--dest", this.host + ":" + this.ports[i] );
    }
}

// List of nodes as host:port strings.
ReplSetTest.prototype.nodeList = function() {
    var list = [];
    for(var i=0; i<this.ports.length; i++) {
      list.push( this.host + ":" + this.ports[i]);
    }

    return list;
}

// Here we store a reference to all reachable nodes.
ReplSetTest.prototype.initLiveNodes = function() {
    this.liveNodes = { master: null, slaves: [] }
}

ReplSetTest.prototype.getNodeId = function(node) {
    
    if( node.toFixed ) return parseInt( node )
    
    for( var i = 0; i < this.nodes.length; i++ ){
        if( this.nodes[i] == node ) return i
    }
    
    if( node instanceof ObjectId ){
        for( var i = 0; i < this.nodes.length; i++ ){
            if( this.nodes[i].runId == node ) return i
        }
    }
    
    if( node.nodeId ) return parseInt( node.nodeId )
    
    return undefined
    
}

ReplSetTest.prototype.getPort = function( n ){
    
    n = this.getNodeId( n )
    
    print( "ReplSetTest n: " + n + " ports: " + tojson( this.ports ) + "\t" + this.ports[n] + " " + typeof(n) );
    return this.ports[ n ];
}

ReplSetTest.prototype.getPath = function( n ){
    
    if( n.host )
        n = this.getNodeId( n )

    var p = "/data/db/" + this.name + "-"+n;
    if ( ! this._alldbpaths )
        this._alldbpaths = [ p ];
    else
        this._alldbpaths.push( p );
    return p;
}

ReplSetTest.prototype.getReplSetConfig = function() {
    var cfg = {};

    cfg['_id']  = this.name;
    cfg.members = [];

    for(i=0; i<this.ports.length; i++) {
        member = {};
        member['_id']  = i;

        if(this.bridged)
          var port = this.bridgePorts[i];
        else
          var port = this.ports[i];

        member['host'] = this.host + ":" + port;
        if( this.nodeOptions[ "n" + i ] && this.nodeOptions[ "n" + i ].arbiter )
            member['arbiterOnly'] = true
            
        cfg.members.push(member);
    }

    return cfg;
}

ReplSetTest.prototype.getURL = function(){
    var hosts = [];
    
    for(i=0; i<this.ports.length; i++) {

        // Don't include this node in the replica set list
        if(this.bridged && this.ports[i] == this.ports[n]) {
            continue;
        }
        
        var port;
        // Connect on the right port
        if(this.bridged) {
            port = this.bridgePorts[i];
        }
        else {
            port = this.ports[i];
        }
        
        var str = this.host + ":" + port;
        hosts.push(str);
    }
    
    return this.name + "/" + hosts.join(",");
}

ReplSetTest.prototype.getOptions = function( n , extra , putBinaryFirst ){

    if ( ! extra )
        extra = {};

    if ( ! extra.oplogSize )
        extra.oplogSize = this.oplogSize;

    var a = []


    if ( putBinaryFirst )
        a.push( "mongod" );

    if ( extra.noReplSet ) {
        delete extra.noReplSet;
    }
    else {
        a.push( "--replSet" );
        
        if( this.useSeedList ) {
            a.push( this.getURL() );
        }
        else {
            a.push( this.name );
        }
    }
    
    a.push( "--noprealloc", "--smallfiles" );

    a.push( "--rest" );

    a.push( "--port" );
    a.push( this.getPort( n ) );

    a.push( "--dbpath" );
    a.push( this.getPath( ( n.host ? this.getNodeId( n ) : n ) ) );
    
    if( this.keyFile ){
        a.push( "--keyFile" )
        a.push( keyFile )
    }        
    
    if( jsTestOptions().noJournal ) a.push( "--nojournal" )
    if( jsTestOptions().noJournalPrealloc ) a.push( "--nopreallocj" )
    if( jsTestOptions().keyFile && !this.keyFile) {
        a.push( "--keyFile" )
        a.push( jsTestOptions().keyFile )
    }
    
    for ( var k in extra ){
        var v = extra[k];
        if( k in MongoRunner.logicalOptions ) continue
        a.push( "--" + k );
        if ( v != null ){
            if( v.replace ){
                v = v.replace(/\$node/g, "" + ( n.host ? this.getNodeId( n ) : n ) )
                v = v.replace(/\$set/g, this.name )
                v = v.replace(/\$path/g, this.getPath( n ) )
            }
            a.push( v );
        }
    }

    return a;
}

ReplSetTest.prototype.startSet = function( options ) {
    
    var nodes = [];
    print( "ReplSetTest Starting Set" );

    for( n = 0 ; n < this.ports.length; n++ ) {
        node = this.start(n, options)
        nodes.push(node);
    }

    this.nodes = nodes;
    return this.nodes;
}

ReplSetTest.prototype.callIsMaster = function() {
  
  var master = null;
  this.initLiveNodes();
    
  for(var i=0; i<this.nodes.length; i++) {

    try {
      var n = this.nodes[i].getDB('admin').runCommand({ismaster:1});
      
      if(n['ismaster'] == true) {
        master = this.nodes[i]
        this.liveNodes.master = master
      }
      else {
        this.nodes[i].setSlaveOk();
        this.liveNodes.slaves.push(this.nodes[i]);
      }

    }
    catch(err) {
      print("ReplSetTest Could not call ismaster on node " + i);
    }
  }

  return master || false;
}

ReplSetTest.awaitRSClientHosts = function( conn, host, hostOk, rs ) {
    var hostCount = host.length;
    if( hostCount ){
        for( var i = 0; i < hostCount; i++ ) {
            ReplSetTest.awaitRSClientHosts( conn, host[i], hostOk, rs );
        }
        return;
    }
    
    if( hostOk == undefined ) hostOk = { ok : true }
    if( host.host ) host = host.host
    if( rs && rs.getMaster ) rs = rs.name
    
    print( "Awaiting " + host + " to be " + tojson( hostOk ) + " for " + conn + " (rs: " + rs + ")" )
    
    var tests = 0
    assert.soon( function() {
        var rsClientHosts = conn.getDB( "admin" ).runCommand( "connPoolStats" )[ "replicaSets" ]
        if( tests++ % 10 == 0 ) 
            printjson( rsClientHosts )
        
        for ( rsName in rsClientHosts ){
            if( rs && rs != rsName ) continue
            for ( var i = 0; i < rsClientHosts[rsName].hosts.length; i++ ){
                var clientHost = rsClientHosts[rsName].hosts[ i ];
                if( clientHost.addr != host ) continue
                
                // Check that *all* host properties are set correctly
                var propOk = true
                for( var prop in hostOk ){
                    if ( isObject( hostOk[prop] )) {
                        if ( !friendlyEqual( hostOk[prop], clientHost[prop] )){
                            propOk = false;
                            break;
                        }
                    }
                    else if ( clientHost[prop] != hostOk[prop] ){
                        propOk = false;
                        break;
                    }
                }
                
                if( propOk ) return true;

            }
        }
        return false;
    }, "timed out waiting for replica set client to recognize hosts",
       3 * 20 * 1000 /* ReplicaSetMonitorWatcher updates every 20s */ )
    
}

ReplSetTest.prototype.awaitSecondaryNodes = function( timeout ) {
  var master = this.getMaster();
  var slaves = this.liveNodes.slaves;
  var len = slaves.length;

  jsTest.attempt({context: this, timeout: 60000, desc: "Awaiting secondaries"}, function() {
     var ready = true;
     for(var i=0; i<len; i++) {
       var isMaster = slaves[i].getDB("admin").runCommand({ismaster: 1});
       var arbiter = isMaster['arbiterOnly'] == undefined ? false : isMaster['arbiterOnly'];
       ready = ready && ( isMaster['secondary'] || arbiter );
     }
     return ready;
  });
}

ReplSetTest.prototype.getMaster = function( timeout ) {
  var tries = 0;
  var sleepTime = 500;
  var t = timeout || 000;
  var master = null;

  master = jsTest.attempt({context: this, timeout: 60000, desc: "Finding master"}, this.callIsMaster);
  return master;
}

ReplSetTest.prototype.getPrimary = ReplSetTest.prototype.getMaster

ReplSetTest.prototype.getSecondaries = function( timeout ){
    var master = this.getMaster( timeout )
    var secs = []
    for( var i = 0; i < this.nodes.length; i++ ){
        if( this.nodes[i] != master ){
            secs.push( this.nodes[i] )
        }
    }
    return secs
}

ReplSetTest.prototype.getSecondary = function( timeout ){
    return this.getSecondaries( timeout )[0];
}

ReplSetTest.prototype.status = function( timeout ){
    var master = this.callIsMaster()
    if( ! master ) master = this.liveNodes.slaves[0]
    return master.getDB("admin").runCommand({replSetGetStatus: 1})
}

// Add a node to the test set
ReplSetTest.prototype.add = function( config ) {
  if(this.ports.length == 0) {
    var nextPort = allocatePorts( 1, this.startPort )[0];
  }
  else {
    var nextPort = this.ports[this.ports.length-1] + 1;
  }
  print("ReplSetTest Next port: " + nextPort);
  this.ports.push(nextPort);
  printjson(this.ports);

  var nextId = this.nodes.length;
  printjson(this.nodes);
  print("ReplSetTest nextId:" + nextId);
  var newNode = this.start( nextId );
  
  return newNode;
}

ReplSetTest.prototype.remove = function( nodeId ) {
    nodeId = this.getNodeId( nodeId )
    this.nodes.splice( nodeId, 1 );
    this.ports.splice( nodeId, 1 );
}

ReplSetTest.prototype.initiate = function( cfg , initCmd , timeout ) {
    var master  = this.nodes[0].getDB("admin");
    var config  = cfg || this.getReplSetConfig();
    var cmd     = {};
    var cmdKey  = initCmd || 'replSetInitiate';
    var timeout = timeout || 30000;
    cmd[cmdKey] = config;
    printjson(cmd);

    jsTest.attempt({context:this, timeout: timeout, desc: "Initiate replica set"}, function() {
        var result = master.runCommand(cmd);
        printjson(result);
        return result['ok'] == 1;
    });

    // Setup authentication if running test with authentication
    if (jsTestOptions().keyFile && !this.keyFile) {
        if (!this.shardSvr) {
            master = this.getMaster();
            jsTest.addAuth(master);
            jsTest.authenticateNodes(this.nodes);
        }
    }
}

ReplSetTest.prototype.reInitiate = function() {
    var master  = this.nodes[0];
    var c = master.getDB("local")['system.replset'].findOne();
    var config  = this.getReplSetConfig();
    config.version = c.version + 1;
    this.initiate( config , 'replSetReconfig' );
}

ReplSetTest.prototype.getLastOpTimeWritten = function() {
    this.getMaster();
    jsTest.attempt({context : this, desc : "awaiting oplog query", timeout: 30000},
                 function() {
                     try {
                         this.latest = this.liveNodes.master.getDB("local")['oplog.rs'].find({}).sort({'$natural': -1}).limit(1).next()['ts'];
                     }
                     catch(e) {
                         print("ReplSetTest caught exception " + e);
                         return false;
                     }
                     return true;
                 });
};

ReplSetTest.prototype.awaitReplication = function(timeout) {
    timeout = timeout || 30000;

    this.getLastOpTimeWritten();

    print("ReplSetTest " + this.latest);

    jsTest.attempt({context: this, timeout: timeout, desc: "awaiting replication"},
                 function() {
                     try {
                         var synced = true;
                         for(var i=0; i<this.liveNodes.slaves.length; i++) {
                             var slave = this.liveNodes.slaves[i];

                             // Continue if we're connected to an arbiter
                             if(res = slave.getDB("admin").runCommand({replSetGetStatus: 1})) {
                                 if(res.myState == 7) {
                                     continue;
                                 }
                             }

                             slave.getDB("admin").getMongo().setSlaveOk();
                             var log = slave.getDB("local")['oplog.rs'];
                             if(log.find({}).sort({'$natural': -1}).limit(1).hasNext()) {
                                 var entry = log.find({}).sort({'$natural': -1}).limit(1).next();
                                 printjson( entry );
                                 var ts = entry['ts'];
                                 print("ReplSetTest await TS for " + slave + " is " + ts.t+":"+ts.i + " and latest is " + this.latest.t+":"+this.latest.i);

                                 if (this.latest.t < ts.t || (this.latest.t == ts.t && this.latest.i < ts.i)) {
                                     this.latest = this.liveNodes.master.getDB("local")['oplog.rs'].find({}).sort({'$natural': -1}).limit(1).next()['ts'];
                                 }

                                 print("ReplSetTest await oplog size for " + slave + " is " + log.count());
                                 synced = (synced && friendlyEqual(this.latest,ts))
                             }
                             else {
                                 print( "ReplSetTest waiting for " + slave + " to have an oplog built." )
                                 synced = false;
                             }
                         }

                         if(synced) {
                             print("ReplSetTest await synced=" + synced);
                         }
                         return synced;
                     }
                     catch (e) {
                         print("ReplSetTest.awaitReplication: caught exception "+e);

                         // we might have a new master now
                         this.getLastOpTimeWritten();

                         return false;
                     }
                 });
}

ReplSetTest.prototype.getHashes = function( db ){
    this.getMaster();
    var res = {};
    res.master = this.liveNodes.master.getDB( db ).runCommand( "dbhash" )
    res.slaves = this.liveNodes.slaves.map( function(z){ return z.getDB( db ).runCommand( "dbhash" ); } )
    return res;
}

/**
 * Starts up a server.  Options are saved by default for subsequent starts.
 * 
 * 
 * Options { remember : true } re-applies the saved options from a prior start.
 * Options { noRemember : true } ignores the current properties.
 * Options { appendOptions : true } appends the current options to those remembered.
 * Options { startClean : true } clears the data directory before starting.
 *
 * @param {int|conn|[int|conn]} n array or single server number (0, 1, 2, ...) or conn
 * @param {object} [options]
 * @param {boolean} [restart] If false, the data directory will be cleared 
 *   before the server starts.  Default: false.
 * 
 */
ReplSetTest.prototype.start = function( n , options , restart , wait ){
    
    if( n.length ){
        
        var nodes = n
        var started = []
        
        for( var i = 0; i < nodes.length; i++ ){
            if( this.start( nodes[i], Object.merge({}, options), restart, wait ) ){
                started.push( nodes[i] )
            }
        }
        
        return started
        
    }
    
    print( "ReplSetTest n is : " + n )
    
    defaults = { useHostName : this.useHostName,
                 oplogSize : this.oplogSize, 
                 keyFile : this.keyFile, 
                 port : this.getPort( n ),
                 noprealloc : "",
                 smallfiles : "",
                 rest : "",
                 replSet : this.useSeedList ? this.getURL() : this.name,
                 dbpath : "$set-$node" }
    
    defaults = Object.merge( defaults, ReplSetTest.nodeOptions || {} )
        
    // TODO : should we do something special if we don't currently know about this node?
    n = this.getNodeId( n )
    
    //
    // Note : this replaces the binVersion of the shared startSet() options the first time 
    // through, so the full set is guaranteed to have different versions if size > 1.  If using
    // start() independently, independent version choices will be made
    //
    if( options && options.binVersion ){
        options.binVersion = 
            MongoRunner.versionIterator( options.binVersion )
    }
    
    options = Object.merge( defaults, options )
    options = Object.merge( options, this.nodeOptions[ "n" + n ] )
    
    options.restart = restart
            
    var pathOpts = { node : n, set : this.name }
    options.pathOpts = Object.merge( options.pathOpts || {}, pathOpts )
    
    if( tojson(options) != tojson({}) )
        printjson(options)

    // make sure to call getPath, otherwise folders wont be cleaned
    this.getPath(n);

    print("ReplSetTest " + (restart ? "(Re)" : "") + "Starting....");
    
    var rval = this.nodes[n] = MongoRunner.runMongod( options )
    
    if( ! rval ) return rval
    
    // Add replica set specific attributes
    this.nodes[n].nodeId = n
            
    printjson( this.nodes )
        
    wait = wait || false
    if( ! wait.toFixed ){
        if( wait ) wait = 0
        else wait = -1
    }
    
    if( wait < 0 ) return rval
    
    // Wait for startup
    this.waitForHealth( rval, this.UP, wait )
    
    return rval
    
}


/**
 * Restarts a db without clearing the data directory by default.  If the server is not
 * stopped first, this function will not work.  
 * 
 * Option { startClean : true } forces clearing the data directory.
 * Option { auth : Object } object that contains the auth details for admin credentials.
 *   Should contain the fields 'user' and 'pwd'
 * 
 * @param {int|conn|[int|conn]} n array or single server number (0, 1, 2, ...) or conn
 */
ReplSetTest.prototype.restart = function( n , options, signal, wait ){
    // Can specify wait as third parameter, if using default signal
    if( signal == true || signal == false ){
        wait = signal
        signal = undefined
    }
    
    this.stop( n, signal, wait && wait.toFixed ? wait : true, options )
    started = this.start( n , options , true, wait );

    if (jsTestOptions().keyFile && !this.keyFile) {
        if (started.length) {
             // if n was an array of conns, start will return an array of connections
            for (var i = 0; i < started.length; i++) {
                jsTest.authenticate(started[i]);
            }
        } else {
            jsTest.authenticate(started);
        }
    }
    return started;
}

ReplSetTest.prototype.stopMaster = function( signal , wait, opts ) {
    var master = this.getMaster();
    var master_id = this.getNodeId( master );
    return this.stop( master_id , signal , wait, opts );
}

/**
 * Stops a particular node or nodes, specified by conn or id
 *
 * @param {number} n the index of the replica set member to stop
 * @param {number} signal the signal number to use for killing
 * @param {boolean} wait
 * @param {Object} opts @see MongoRunner.stopMongod
 */
ReplSetTest.prototype.stop = function( n , signal, wait /* wait for stop */, opts ){
        
    // Flatten array of nodes to stop
    if( n.length ){
        nodes = n
        
        var stopped = []
        for( var i = 0; i < nodes.length; i++ ){
            if( this.stop( nodes[i], signal, wait, opts ) )
                stopped.push( nodes[i] )
        }
        
        return stopped
    }
    
    // Can specify wait as second parameter, if using default signal
    if( signal == true || signal == false ){
        wait = signal
        signal = undefined
    }
        
    wait = wait || false
    if( ! wait.toFixed ){
        if( wait ) wait = 0
        else wait = -1
    }
    
    var port = this.getPort( n );
    print('ReplSetTest stop *** Shutting down mongod in port ' + port + ' ***');
    var ret = MongoRunner.stopMongod( port , signal, opts );
    
    if( ! ret || wait < 0 ) return ret
    
    // Wait for shutdown
    this.waitForHealth( n, this.DOWN, wait )
    
    return true
}

/**
 * Kill all members of this replica set.
 *
 * @param {number} signal The signal number to use for killing the members
 * @param {boolean} forRestart will not cleanup data directory or teardown
 *   bridges if set to true.
 * @param {Object} opts @see MongoRunner.stopMongod
 */
ReplSetTest.prototype.stopSet = function( signal , forRestart, opts ) {
    for(var i=0; i < this.ports.length; i++) {
        this.stop( i, signal, false, opts );
    }
    if ( forRestart ) { return; }
    if ( this._alldbpaths ){
        print("ReplSetTest stopSet deleting all dbpaths");
        for( i=0; i<this._alldbpaths.length; i++ ){
            resetDbpath( this._alldbpaths[i] );
        }
    }
    if ( this.bridges ) {
        var mybridgevec;
        while (mybridgevec = this.bridges.pop()) {
            var mybridge;
            while (mybridge = mybridgevec.pop()) {
                mybridge.stop();
            }       
        }
    }
    
    print('ReplSetTest stopSet *** Shut down repl set - test worked ****' )
};


/**
 * Waits until there is a master node
 */
ReplSetTest.prototype.waitForMaster = function( timeout ){
    
    var master = undefined
    
    jsTest.attempt({context: this, timeout: timeout, desc: "waiting for master"}, function() {
        return ( master = this.getMaster() )
    });
    
    return master
}


/**
 * Wait for a health indicator to go to a particular state or states.
 * 
 * @param node is a single node or list of nodes, by id or conn
 * @param state is a single state or list of states
 * 
 */
ReplSetTest.prototype.waitForHealth = function( node, state, timeout ){
    this.waitForIndicator( node, state, "health", timeout )    
}

/**
 * Wait for a state indicator to go to a particular state or states.
 * 
 * @param node is a single node or list of nodes, by id or conn
 * @param state is a single state or list of states
 * 
 */
ReplSetTest.prototype.waitForState = function( node, state, timeout ){
    this.waitForIndicator( node, state, "state", timeout )
}

/**
 * Wait for a rs indicator to go to a particular state or states.
 * 
 * @param node is a single node or list of nodes, by id or conn
 * @param states is a single state or list of states
 * @param ind is the indicator specified
 * 
 */
ReplSetTest.prototype.waitForIndicator = function( node, states, ind, timeout ){
    
    if( node.length ){
        
        var nodes = node        
        for( var i = 0; i < nodes.length; i++ ){
            if( states.length )
                this.waitForIndicator( nodes[i], states[i], ind, timeout )
            else
                this.waitForIndicator( nodes[i], states, ind, timeout )
        }
        
        return;
    }    
    
    timeout = timeout || 30000;
    
    if( ! node.getDB ){
        node = this.nodes[node]
    }
    
    if( ! states.length ) states = [ states ]
    
    print( "ReplSetTest waitForIndicator " + ind + " on " + node )
    printjson( states )
    print( "ReplSetTest waitForIndicator from node " + node )
    
    var lastTime = null
    var currTime = new Date().getTime()
    var status = undefined
        
    jsTest.attempt({context: this, timeout: timeout, desc: "waiting for state indicator " + ind + " for " + timeout + "ms" }, function() {
        
        status = this.status()
        
        var printStatus = false
        if( lastTime == null || ( currTime = new Date().getTime() ) - (1000 * 5) > lastTime ){
            if( lastTime == null ) print( "ReplSetTest waitForIndicator Initial status ( timeout : " + timeout + " ) :" )
            printjson( status )
            lastTime = new Date().getTime()
            printStatus = true
        }

        if (typeof status.members == 'undefined') {
            return false;
        }

        for( var i = 0; i < status.members.length; i++ ){
            if( printStatus ) print( "Status for : " + status.members[i].name + ", checking " + node.host + "/" + node.name )
            if( status.members[i].name == node.host || status.members[i].name == node.name ){
                for( var j = 0; j < states.length; j++ ){
                    if( printStatus ) print( "Status " + " : " + status.members[i][ind] + "  target state : " + states[j] )
                    if( status.members[i][ind] == states[j] ) return true;
                }
            }
        }
        
        return false
        
    });
    
    print( "ReplSetTest waitForIndicator final status:" )
    printjson( status )
    
}

ReplSetTest.Health = {}
ReplSetTest.Health.UP = 1
ReplSetTest.Health.DOWN = 0

ReplSetTest.State = {}
ReplSetTest.State.PRIMARY = 1
ReplSetTest.State.SECONDARY = 2
ReplSetTest.State.RECOVERING = 3
ReplSetTest.State.ARBITER = 7

/** 
 * Overflows a replica set secondary or secondaries, specified by id or conn.
 */
ReplSetTest.prototype.overflow = function( secondaries ){
    
    // Create a new collection to overflow, allow secondaries to replicate
    var master = this.getMaster()
    var overflowColl = master.getCollection( "_overflow.coll" )
    overflowColl.insert({ replicated : "value" })
    this.awaitReplication()
    
    this.stop( secondaries, undefined, 5 * 60 * 1000 )
        
    var count = master.getDB("local").oplog.rs.count();
    var prevCount = -1;
    
    // Keep inserting till we hit our capped coll limits
    while (count != prevCount) {
      
      print("ReplSetTest overflow inserting 10000");
      
      for (var i = 0; i < 10000; i++) {
          overflowColl.insert({ overflow : "value" });
      }
      prevCount = count;
      this.awaitReplication();
      
      count = master.getDB("local").oplog.rs.count();
      
      print( "ReplSetTest overflow count : " + count + " prev : " + prevCount );
      
    }
    
    // Restart all our secondaries and wait for recovery state
    this.start( secondaries, { remember : true }, true, true )
    this.waitForState( secondaries, this.RECOVERING, 5 * 60 * 1000 )
    
}




/**
 * Bridging allows you to test network partitioning.  For example, you can set
 * up a replica set, run bridge(), then kill the connection between any two
 * nodes x and y with partition(x, y).
 *
 * Once you have called bridging, you cannot reconfigure the replica set.
 */
ReplSetTest.prototype.bridge = function( opts ) {
    if (this.bridges) {
        print("ReplSetTest bridge bridges have already been created!");
        return;
    }
    
    var n = this.nodes.length;

    // create bridges
    this.bridges = [];
    for (var i=0; i<n; i++) {
        var nodeBridges = [];
        for (var j=0; j<n; j++) {
            if (i == j) {
                continue;
            }
            nodeBridges[j] = new ReplSetBridge(this, i, j);
        }
        this.bridges.push(nodeBridges);
    }
    print("ReplSetTest bridge bridges: " + this.bridges);
    
    // restart everyone independently
    this.stopSet(null, true, opts );
    for (var i=0; i<n; i++) {
        this.restart(i, {noReplSet : true});
    }
    
    // create new configs
    for (var i=0; i<n; i++) {
        config = this.nodes[i].getDB("local").system.replset.findOne();
        
        if (!config) {
            print("ReplSetTest bridge couldn't find config for "+this.nodes[i]);
            printjson(this.nodes[i].getDB("local").system.namespaces.find().toArray());
            assert(false);
        }

        var updateMod = {"$set" : {}};
        for (var j = 0; j<config.members.length; j++) {
            if (config.members[j].host == this.host+":"+this.ports[i]) {
                continue;
            }

            updateMod['$set']["members."+j+".host"] = this.bridges[i][j].host;
        }
        print("ReplSetTest bridge for node " + i + ":");
        printjson(updateMod);
        this.nodes[i].getDB("local").system.replset.update({},updateMod);
    }

    this.stopSet( null, true, opts );
    
    // start set
    for (var i=0; i<n; i++) {
        this.restart(i);
    }

    return this.getMaster();
};

/**
 * This kills the bridge between two nodes.  As parameters, specify the from and
 * to node numbers.
 *
 * For example, with a three-member replica set, we'd have nodes 0, 1, and 2,
 * with the following bridges: 0->1, 0->2, 1->0, 1->2, 2->0, 2->1.  We can kill
 * the connection between nodes 0 and 2 by calling replTest.partition(0,2) or
 * replTest.partition(2,0) (either way is identical). Then the replica set would
 * have the following bridges: 0->1, 1->0, 1->2, 2->1.
 */
ReplSetTest.prototype.partition = function(from, to) {
    this.bridges[from][to].stop();
    this.bridges[to][from].stop();
};

/**
 * This reverses a partition created by partition() above.
 */
ReplSetTest.prototype.unPartition = function(from, to) {
    this.bridges[from][to].start();
    this.bridges[to][from].start();
};
/**
 * Run a mongod process.
 *
 * After initializing a MongodRunner, you must call start() on it.
 * @param {int} port port to run db on, use allocatePorts(num) to requision
 * @param {string} dbpath path to use
 * @param {boolean} peer pass in false (DEPRECATED, was used for replica pair host)
 * @param {boolean} arbiter pass in false (DEPRECATED, was used for replica pair host)
 * @param {array} extraArgs other arguments for the command line
 * @param {object} options other options include no_bind to not bind_ip to 127.0.0.1
 *    (necessary for replica set testing)
 */
MongodRunner = function( port, dbpath, peer, arbiter, extraArgs, options ) {
    this.port_ = port;
    this.dbpath_ = dbpath;
    this.peer_ = peer;
    this.arbiter_ = arbiter;
    this.extraArgs_ = extraArgs;
    this.options_ = options ? options : {};
};

/**
 * Start this mongod process.
 *
 * @param {boolean} reuseData If the data directory should be left intact (default is to wipe it)
 */
MongodRunner.prototype.start = function( reuseData ) {
    var args = [];
    if ( reuseData ) {
        args.push( "mongod" );
    }
    args.push( "--port" );
    args.push( this.port_ );
    args.push( "--dbpath" );
    args.push( this.dbpath_ );
    args.push( "--nohttpinterface" );
    args.push( "--noprealloc" );
    args.push( "--smallfiles" );
    if (!this.options_.no_bind) {
      args.push( "--bind_ip" );
      args.push( "127.0.0.1" );
    }
    if ( this.extraArgs_ ) {
        args = args.concat( this.extraArgs_ );
    }
    removeFile( this.dbpath_ + "/mongod.lock" );
    if ( reuseData ) {
        return startMongoProgram.apply( null, args );
    } else {
        return startMongod.apply( null, args );
    }
}

MongodRunner.prototype.port = function() { return this.port_; }

MongodRunner.prototype.toString = function() { return [ this.port_, this.dbpath_, this.peer_, this.arbiter_ ].toString(); }

ToolTest = function( name, extraOptions ){
    this.name = name;
    this.options = extraOptions;
    this.port = allocatePorts(1)[0];
    this.baseName = "jstests_tool_" + name;
    this.root = "/data/db/" + this.baseName;
    this.dbpath = this.root + "/";
    this.ext = this.root + "_external/";
    this.extFile = this.root + "_external/a";
    resetDbpath( this.dbpath );
    resetDbpath( this.ext );
}

ToolTest.prototype.startDB = function( coll ){
    assert( ! this.m , "db already running" );

    var options = {port : this.port,
                   dbpath : this.dbpath,
                   nohttpinterface : "",
                   noprealloc : "",
                   smallfiles : "",
                   bind_ip : "127.0.0.1"};

    Object.extend(options, this.options);

    this.m = startMongoProgram.apply(null, MongoRunner.arrOptions("mongod", options));
    this.db = this.m.getDB( this.baseName );
    if ( coll )
        return this.db.getCollection( coll );
    return this.db;
}

ToolTest.prototype.stop = function(){
    if ( ! this.m )
        return;
    stopMongod( this.port );
    this.m = null;
    this.db = null;

    print('*** ' + this.name + " completed successfully ***");
}

ToolTest.prototype.runTool = function(){
    var a = [ "mongo" + arguments[0] ];

    var hasdbpath = false;
    
    for ( var i=1; i<arguments.length; i++ ){
        a.push( arguments[i] );
        if ( arguments[i] == "--dbpath" )
            hasdbpath = true;
    }

    if ( ! hasdbpath ){
        a.push( "--host" );
        a.push( "127.0.0.1:" + this.port );
    }

    return runMongoProgram.apply( null , a );
}


ReplTest = function( name, ports ){
    this.name = name;
    this.ports = ports || allocatePorts( 2 );
}

ReplTest.prototype.getPort = function( master ){
    if ( master )
        return this.ports[ 0 ];
    return this.ports[ 1 ]
}

ReplTest.prototype.getPath = function( master ){
    var p = "/data/db/" + this.name + "-";
    if ( master )
        p += "master";
    else
        p += "slave"
    return p;
}

ReplTest.prototype.getOptions = function( master , extra , putBinaryFirst, norepl ){

    if ( ! extra )
        extra = {};

    if ( ! extra.oplogSize )
        extra.oplogSize = "40";
        
    var a = []
    if ( putBinaryFirst )
        a.push( "mongod" )
    a.push( "--nohttpinterface", "--noprealloc", "--bind_ip" , "127.0.0.1" , "--smallfiles" );

    a.push( "--port" );
    a.push( this.getPort( master ) );

    a.push( "--dbpath" );
    a.push( this.getPath( master ) );
    
    if( jsTestOptions().noJournal ) a.push( "--nojournal" )
    if( jsTestOptions().noJournalPrealloc ) a.push( "--nopreallocj" )
    if( jsTestOptions().keyFile ) {
        a.push( "--keyFile" )
        a.push( jsTestOptions().keyFile )
    }

    if ( !norepl ) {
        if ( master ){
            a.push( "--master" );
        }
        else {
            a.push( "--slave" );
            a.push( "--source" );
            a.push( "127.0.0.1:" + this.ports[0] );
        }
    }
    
    for ( var k in extra ){
        var v = extra[k];
        if( k in MongoRunner.logicalOptions ) continue
        a.push( "--" + k );
        if ( v != null )
            a.push( v );                    
    }

    return a;
}

ReplTest.prototype.start = function( master , options , restart, norepl ){
    var lockFile = this.getPath( master ) + "/mongod.lock";
    removeFile( lockFile );
    var o = this.getOptions( master , options , restart, norepl );


    if ( restart )
        return startMongoProgram.apply( null , o );
    else
        return startMongod.apply( null , o );
}

ReplTest.prototype.stop = function( master , signal ){
    if ( arguments.length == 0 ){
        this.stop( true );
        this.stop( false );
        return;
    }

    print('*** ' + this.name + " completed successfully ***");
    return stopMongod( this.getPort( master ) , signal || 15 );
}

allocatePorts = function( n , startPort ) {
    var ret = [];
    var start = startPort || 31000;
    for( var i = start; i < start + n; ++i )
        ret.push( i );
    return ret;
}


sh = function() { return "try sh.help();" }

sh._checkMongos = function() {
    var x = db.runCommand( "ismaster" );
    if ( x.msg != "isdbgrid" )
        throw "not connected to a mongos"
}

sh._checkFullName = function( fullName ) {
    assert( fullName , "neeed a full name" )
    assert( fullName.indexOf( "." ) > 0 , "name needs to be fully qualified <db>.<collection>'" )
}

sh._adminCommand = function( cmd , skipCheck ) {
    if ( ! skipCheck ) sh._checkMongos();
    return db.getSisterDB( "admin" ).runCommand( cmd );
}

sh._dataFormat = function( bytes ){
   if( bytes < 1024 ) return Math.floor( bytes ) + "b"
   if( bytes < 1024 * 1024 ) return Math.floor( bytes / 1024 ) + "kb"
   if( bytes < 1024 * 1024 * 1024 ) return Math.floor( ( Math.floor( bytes / 1024 ) / 1024 ) * 100 ) / 100 + "Mb"
   return Math.floor( ( Math.floor( bytes / ( 1024 * 1024 ) ) / 1024 ) * 100 ) / 100 + "Gb"
}

sh._collRE = function( coll ){
   return RegExp( "^" + RegExp.escape(coll + "") + "-.*" )
}

sh._pchunk = function( chunk ){
   return "[" + tojson( chunk.min ) + " -> " + tojson( chunk.max ) + "]"
}

sh.help = function() {
    print( "\tsh.addShard( host )                       server:port OR setname/server:port" )
    print( "\tsh.enableSharding(dbname)                 enables sharding on the database dbname" )
    print( "\tsh.shardCollection(fullName,key,unique)   shards the collection" );

    print( "\tsh.splitFind(fullName,find)               splits the chunk that find is in at the median" );
    print( "\tsh.splitAt(fullName,middle)               splits the chunk that middle is in at middle" );
    print( "\tsh.moveChunk(fullName,find,to)            move the chunk where 'find' is to 'to' (name of shard)");
    
    print( "\tsh.setBalancerState( <bool on or not> )   turns the balancer on or off true=on, false=off" );
    print( "\tsh.getBalancerState()                     return true if on, off if not" );
    print( "\tsh.isBalancerRunning()                    return true if the balancer is running on any mongos" );

    print( "\tsh.addShardTag(shard,tag)                 adds the tag to the shard" );
    print( "\tsh.removeShardTag(shard,tag)              removes the tag from the shard" );
    
    print( "\tsh.status()                               prints a general overview of the cluster" )
}

sh.status = function( verbose , configDB ) { 
    // TODO: move the actual commadn here
    printShardingStatus( configDB , verbose );
}

sh.addShard = function( url ){
    return sh._adminCommand( { addShard : url } , true );
}

sh.enableSharding = function( dbname ) { 
    assert( dbname , "need a valid dbname" )
    return sh._adminCommand( { enableSharding : dbname } );
}

sh.shardCollection = function( fullName , key , unique ) {
    sh._checkFullName( fullName )
    assert( key , "need a key" )
    assert( typeof( key ) == "object" , "key needs to be an object" )
    
    var cmd = { shardCollection : fullName , key : key }
    if ( unique ) 
        cmd.unique = true;

    return sh._adminCommand( cmd );
}

sh.splitFind = function( fullName , find ) {
    sh._checkFullName( fullName )
    return sh._adminCommand( { split : fullName , find : find } );
}

sh.splitAt = function( fullName , middle ) {
    sh._checkFullName( fullName )
    return sh._adminCommand( { split : fullName , middle : middle } );
}

sh.moveChunk = function( fullName , find , to ) {
    sh._checkFullName( fullName );
    return sh._adminCommand( { moveChunk : fullName , find : find , to : to } )
}

sh.setBalancerState = function( onOrNot ) { 
    db.getSisterDB( "config" ).settings.update({ _id: "balancer" }, { $set : { stopped: onOrNot ? false : true } }, true );
}

sh.getBalancerState = function() {
    var x = db.getSisterDB( "config" ).settings.findOne({ _id: "balancer" } )
    if ( x == null )
        return true;
    return ! x.stopped;
}

sh.isBalancerRunning = function () {
    var x = db.getSisterDB("config").locks.findOne({ _id: "balancer" });
    if (x == null) {
        print("config.locks collection empty or missing. be sure you are connected to a mongos");
        return false;
    }
    return x.state > 0;
}

sh.getBalancerHost = function() {   
    var x = db.getSisterDB("config").locks.findOne({ _id: "balancer" });
    if( x == null ){
        print("config.locks collection does not contain balancer lock. be sure you are connected to a mongos");
        return ""
    }
    return x.process.match(/[^:]+:[^:]+/)[0]
}

sh.stopBalancer = function( timeout, interval ) {
    sh.setBalancerState( false )
    sh.waitForBalancer( false, timeout, interval )
}

sh.startBalancer = function( timeout, interval ) {
    sh.setBalancerState( true )
    sh.waitForBalancer( true, timeout, interval )
}

sh.waitForDLock = function( lockId, onOrNot, timeout, interval ){
    
    // Wait for balancer to be on or off
    // Can also wait for particular balancer state
    var state = onOrNot
    
    var beginTS = undefined
    if( state == undefined ){
        var currLock = db.getSisterDB( "config" ).locks.findOne({ _id : lockId })
        if( currLock != null ) beginTS = currLock.ts
    }
        
    var lockStateOk = function(){
        var lock = db.getSisterDB( "config" ).locks.findOne({ _id : lockId })

        if( state == false ) return ! lock || lock.state == 0
        if( state == true ) return lock && lock.state == 2
        if( state == undefined ) return (beginTS == undefined && lock) || 
                                        (beginTS != undefined && ( !lock || lock.ts + "" != beginTS + "" ) )
        else return lock && lock.state == state
    }
    
    assert.soon( lockStateOk,
                 "Waited too long for lock " + lockId + " to " + 
                      (state == true ? "lock" : ( state == false ? "unlock" : 
                                       "change to state " + state ) ),
                 timeout,
                 interval
    )
}

sh.waitForPingChange = function( activePings, timeout, interval ){
    
    var isPingChanged = function( activePing ){
        var newPing = db.getSisterDB( "config" ).mongos.findOne({ _id : activePing._id })
        return ! newPing || newPing.ping + "" != activePing.ping + ""
    }
    
    // First wait for all active pings to change, so we're sure a settings reload
    // happened
    
    // Timeout all pings on the same clock
    var start = new Date()
    
    var remainingPings = []
    for( var i = 0; i < activePings.length; i++ ){
        
        var activePing = activePings[ i ]
        print( "Waiting for active host " + activePing._id + " to recognize new settings... (ping : " + activePing.ping + ")" )
       
        // Do a manual timeout here, avoid scary assert.soon errors
        var timeout = timeout || 30000;
        var interval = interval || 200;
        while( isPingChanged( activePing ) != true ){
            if( ( new Date() ).getTime() - start.getTime() > timeout ){
                print( "Waited for active ping to change for host " + activePing._id + 
                       ", a migration may be in progress or the host may be down." )
                remainingPings.push( activePing )
                break
            }
            sleep( interval )   
        }
    
    }
    
    return remainingPings
}

sh.waitForBalancerOff = function( timeout, interval ){
    
    var pings = db.getSisterDB( "config" ).mongos.find().toArray()
    var activePings = []
    for( var i = 0; i < pings.length; i++ ){
        if( ! pings[i].waiting ) activePings.push( pings[i] )
    }
    
    print( "Waiting for active hosts..." )
    
    activePings = sh.waitForPingChange( activePings, 60 * 1000 )
    
    // After 1min, we assume that all hosts with unchanged pings are either 
    // offline (this is enough time for a full errored balance round, if a network
    // issue, which would reload settings) or balancing, which we wait for next
    // Legacy hosts we always have to wait for
    
    print( "Waiting for the balancer lock..." )
    
    // Wait for the balancer lock to become inactive
    // We can guess this is stale after 15 mins, but need to double-check manually
    try{ 
        sh.waitForDLock( "balancer", false, 15 * 60 * 1000 )
    }
    catch( e ){
        print( "Balancer still may be active, you must manually verify this is not the case using the config.changelog collection." )
        throw e
    }
        
    print( "Waiting again for active hosts after balancer is off..." )
    
    // Wait a short time afterwards, to catch the host which was balancing earlier
    activePings = sh.waitForPingChange( activePings, 5 * 1000 )
    
    // Warn about all the stale host pings remaining
    for( var i = 0; i < activePings.length; i++ ){
        print( "Warning : host " + activePings[i]._id + " seems to have been offline since " + activePings[i].ping )
    }
    
}

sh.waitForBalancer = function( onOrNot, timeout, interval ){
    
    // If we're waiting for the balancer to turn on or switch state or
    // go to a particular state
    if( onOrNot ){
        // Just wait for the balancer lock to change, can't ensure we'll ever see it
        // actually locked
        sh.waitForDLock( "balancer", undefined, timeout, interval )
    }
    else {
        // Otherwise we need to wait until we're sure balancing stops
        sh.waitForBalancerOff( timeout, interval )
    }
    
}

sh.disableBalancing = function( coll ){
    var dbase = db
    if( coll instanceof DBCollection ) dbase = coll.getDB()
    dbase.getSisterDB( "config" ).collections.update({ _id : coll + "" }, { $set : { "noBalance" : true } })
}

sh.enableBalancing = function( coll ){
    var dbase = db
    if( coll instanceof DBCollection ) dbase = coll.getDB()
    dbase.getSisterDB( "config" ).collections.update({ _id : coll + "" }, { $set : { "noBalance" : false } })
}

/*
 * Can call _lastMigration( coll ), _lastMigration( db ), _lastMigration( st ), _lastMigration( mongos ) 
 */
sh._lastMigration = function( ns ){
    
    var coll = null
    var dbase = null
    var config = null
    
    if( ! ns ){
        config = db.getSisterDB( "config" )
    }   
    else if( ns instanceof DBCollection ){
        coll = ns
        config = coll.getDB().getSisterDB( "config" )
    }
    else if( ns instanceof DB ){
        dbase = ns
        config = dbase.getSisterDB( "config" )
    }
    else if( ns instanceof ShardingTest ){
        config = ns.s.getDB( "config" )
    }
    else if( ns instanceof Mongo ){
        config = ns.getDB( "config" )
    }
    else {
        // String namespace
        ns = ns + ""
        if( ns.indexOf( "." ) > 0 ){
            config = db.getSisterDB( "config" )
            coll = db.getMongo().getCollection( ns )
        }
        else{
            config = db.getSisterDB( "config" )
            dbase = db.getSisterDB( ns )
        }
    }
        
    var searchDoc = { what : /^moveChunk/ }
    if( coll ) searchDoc.ns = coll + ""
    if( dbase ) searchDoc.ns = new RegExp( "^" + dbase + "\\." )
        
    var cursor = config.changelog.find( searchDoc ).sort({ time : -1 }).limit( 1 )
    if( cursor.hasNext() ) return cursor.next()
    else return null
}

sh._checkLastError = function( mydb ) {
    var err = mydb.getLastError();
    if ( err )
        throw "error: " + err;
}

sh.addShardTag = function( shard, tag ) {
    var config = db.getSisterDB( "config" );
    if ( config.shards.findOne( { _id : shard } ) == null ) {
        throw "can't find a shard with name: " + shard;
    }
    config.shards.update( { _id : shard } , { $addToSet : { tags : tag } } );
    sh._checkLastError( config );
}

sh.removeShardTag = function( shard, tag ) {
    var config = db.getSisterDB( "config" );
    if ( config.shards.findOne( { _id : shard } ) == null ) {
        throw "can't find a shard with name: " + shard;
    }
    config.shards.update( { _id : shard } , { $pull : { tags : tag } } );
    sh._checkLastError( config );
}

sh.addTagRange = function( ns, min, max, tag ) {
    var config = db.getSisterDB( "config" );
    config.tags.update( {_id: { ns : ns , min : min } } , 
            {_id: { ns : ns , min : min }, ns : ns , min : min , max : max , tag : tag } , 
            true );
    sh._checkLastError( config );    
}
__quiet = false;
__magicNoPrint = { __magicNoPrint : 1111 }
__callLastError = false; 
_verboseShell = false;

chatty = function(s){
    if ( ! __quiet )
        print( s );
}

friendlyEqual = function( a , b ){
    if ( a == b )
        return true;
    
    a = tojson(a,false,true);
    b = tojson(b,false,true);

    if ( a == b )
        return true;

    var clean = function( s ){
        s = s.replace( /NumberInt\((\-?\d+)\)/g , "$1" );
        return s;
    }
    
    a = clean(a);
    b = clean(b);

    if ( a == b )
        return true;
    
    return false;
}

printStackTrace = function(){
    try{
        throw new Error("Printing Stack Trace");
    } catch (e) {
        print(e.stack);
    }
}

/**
 * <p> Set the shell verbosity. If verbose the shell will display more information about command results. </>
 * <p> Default is off. <p>
 * @param {Bool} verbosity on / off
 */
setVerboseShell = function( value ) { 
    if( value == undefined ) value = true; 
    _verboseShell = value; 
}

doassert = function (msg) {
    if (msg.indexOf("assert") == 0)
        print(msg);
    else
        print("assert: " + msg);
    printStackTrace();
    throw msg;
}

assert = function( b , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );
    if ( b )
        return;    
    doassert( msg == undefined ? "assert failed" : "assert failed : " + msg );
}

// the mongo code uses verify
// so this is to be nice to mongo devs
verify = assert;

assert.automsg = function( b ) {
    assert( eval( b ), b );
}

assert._debug = false;

assert.eq = function( a , b , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( a == b )
        return;

    if ( ( a != null && b != null ) && friendlyEqual( a , b ) )
        return;

    doassert( "[" + tojson( a ) + "] != [" + tojson( b ) + "] are not equal : " + msg );
}

assert.eq.automsg = function( a, b ) {
    assert.eq( eval( a ), eval( b ), "[" + a + "] != [" + b + "]" );
}

assert.neq = function( a , b , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );
    if ( a != b )
        return;

    doassert( "[" + a + "] != [" + b + "] are equal : " + msg );
}

assert.contains = function( o, arr, msg ){
    var wasIn = false
    
    if( ! arr.length ){
        for( var i in arr ){
            wasIn = arr[i] == o || ( ( arr[i] != null && o != null ) && friendlyEqual( arr[i] , o ) )
                return;
            if( wasIn ) break
        }
    }
    else {
        for( var i = 0; i < arr.length; i++ ){
            wasIn = arr[i] == o || ( ( arr[i] != null && o != null ) && friendlyEqual( arr[i] , o ) )
            if( wasIn ) break
        }
    }
    
    if( ! wasIn ) doassert( tojson( o ) + " was not in " + tojson( arr ) + " : " + msg )
}

assert.repeat = function( f, msg, timeout, interval ) {
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    var start = new Date();
    timeout = timeout || 30000;
    interval = interval || 200;
    var last;
    while( 1 ) {
        
        if ( typeof( f ) == "string" ){
            if ( eval( f ) )
                return;
        }
        else {
            if ( f() )
                return;
        }
        
        if ( ( new Date() ).getTime() - start.getTime() > timeout )
            break;
        sleep( interval );
    }
}
    
assert.soon = function( f, msg, timeout /*ms*/, interval ) {
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    var start = new Date();
    timeout = timeout || 30000;
    interval = interval || 200;
    var last;
    while( 1 ) {
        
        if ( typeof( f ) == "string" ){
            if ( eval( f ) )
                return;
        }
        else {
            if ( f() )
                return;
        }
       
        diff = ( new Date() ).getTime() - start.getTime();
        if ( diff > timeout )
            doassert( "assert.soon failed: " + f + ", msg:" + msg );
        sleep( interval );
    }
}

assert.time = function( f, msg, timeout /*ms*/ ) {
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    var start = new Date();
    timeout = timeout || 30000;
        
        if ( typeof( f ) == "string" ){
            res = eval( f );
        }
        else {
            res = f();
        }
       
        diff = ( new Date() ).getTime() - start.getTime();
        if ( diff > timeout )
            doassert( "assert.time failed timeout " + timeout + "ms took " + diff + "ms : " + f + ", msg:" + msg );
        return res;
}

assert.throws = function( func , params , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );
    
    if ( params && typeof( params ) == "string" )
        throw "2nd argument to assert.throws has to be an array"
    
    try {
        func.apply( null , params );
    }
    catch ( e ){
        return e;
    }

    doassert( "did not throw exception: " + msg );
}

assert.throws.automsg = function( func, params ) {
    assert.throws( func, params, func.toString() );
}

assert.commandWorked = function( res , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( res.ok == 1 )
        return;
    
    doassert( "command failed: " + tojson( res ) + " : " + msg );
}

assert.commandFailed = function( res , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( res.ok == 0 )
        return;
    
    doassert( "command worked when it should have failed: " + tojson( res ) + " : " + msg );
}

assert.isnull = function( what , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( what == null )
        return;
    
    doassert( "supposed to be null (" + ( msg || "" ) + ") was: " + tojson( what ) );
}

assert.lt = function( a , b , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( a < b )
        return;
    doassert( a + " is not less than " + b + " : " + msg );
}

assert.gt = function( a , b , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( a > b )
        return;
    doassert( a + " is not greater than " + b + " : " + msg );
}

assert.lte = function( a , b , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( a <= b )
        return;
    doassert( a + " is not less than or eq " + b + " : " + msg );
}

assert.gte = function( a , b , msg ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if ( a >= b )
        return;
    doassert( a + " is not greater than or eq " + b + " : " + msg );
}

assert.between = function( a, b, c, msg, inclusive ){
    if ( assert._debug && msg ) print( "in assert for: " + msg );

    if( ( inclusive == undefined || inclusive == true ) &&
        a <= b && b <= c ) return;
    else if( a < b && b < c ) return;
    
    doassert( b + " is not between " + a + " and " + c + " : " + msg );
}

assert.betweenIn = function( a, b, c, msg ){ assert.between( a, b, c, msg, true ) }
assert.betweenEx = function( a, b, c, msg ){ assert.between( a, b, c, msg, false ) }

assert.close = function( a , b , msg , places ){
    if (places === undefined) {
        places = 4;
    }
    if (Math.round((a - b) * Math.pow(10, places)) === 0) {
        return;
    }
    doassert( a + " is not equal to " + b + " within " + places +
              " places, diff: " + (a-b) + " : " + msg );
};

Object.extend = function( dst , src , deep ){
    for ( var k in src ){
        var v = src[k];
        if ( deep && typeof(v) == "object" ){
            if ( "floatApprox" in v ) { // convert NumberLong properly
                eval( "v = " + tojson( v ) );
            } else {
                v = Object.extend( typeof ( v.length ) == "number" ? [] : {} , v , true );
            }
        }
        dst[k] = v;
    }
    return dst;
}

Object.merge = function( dst, src, deep ){
    var clone = Object.extend( {}, dst, deep )
    return Object.extend( clone, src, deep )
}

argumentsToArray = function( a ){
    var arr = [];
    for ( var i=0; i<a.length; i++ )
        arr[i] = a[i];
    return arr;
}

isString = function( x ){
    return typeof( x ) == "string";
}

isNumber = function(x){
    return typeof( x ) == "number";
}

isObject = function( x ){
    return typeof( x ) == "object";
}

String.prototype.trim = function() {
    return this.replace(/^\s+|\s+$/g,"");
}
String.prototype.ltrim = function() {
    return this.replace(/^\s+/,"");
}
String.prototype.rtrim = function() {
    return this.replace(/\s+$/,"");
}

String.prototype.startsWith = function (str){
    return this.indexOf(str) == 0
}

String.prototype.endsWith = function (str){
    return new RegExp( RegExp.escape(str) + "$" ).test( this )
}

Number.prototype.zeroPad = function(width) {
    var str = this + '';
    while (str.length < width)
        str = '0' + str;
    return str;
}

Date.timeFunc = function( theFunc , numTimes ){

    var start = new Date();
    
    numTimes = numTimes || 1;
    for ( var i=0; i<numTimes; i++ ){
        theFunc.apply( null , argumentsToArray( arguments ).slice( 2 ) );
    }

    return (new Date()).getTime() - start.getTime();
}

Date.prototype.tojson = function(){

    var UTC = Date.printAsUTC ? 'UTC' : '';

    var year = this['get'+UTC+'FullYear']().zeroPad(4);
    var month = (this['get'+UTC+'Month']() + 1).zeroPad(2);
    var date = this['get'+UTC+'Date']().zeroPad(2);
    var hour = this['get'+UTC+'Hours']().zeroPad(2);
    var minute = this['get'+UTC+'Minutes']().zeroPad(2);
    var sec = this['get'+UTC+'Seconds']().zeroPad(2)

    if (this['get'+UTC+'Milliseconds']())
        sec += '.' + this['get'+UTC+'Milliseconds']().zeroPad(3)

    var ofs = 'Z';
    if (!Date.printAsUTC){
        var ofsmin = this.getTimezoneOffset();
        if (ofsmin != 0){
            ofs = ofsmin > 0 ? '-' : '+'; // This is correct
            ofs += (ofsmin/60).zeroPad(2)
            ofs += (ofsmin%60).zeroPad(2)
        }
    }

    return 'ISODate("'+year+'-'+month+'-'+date+'T'+hour+':'+minute+':'+sec+ofs+'")';
}

Date.printAsUTC = true;


ISODate = function(isoDateStr){
    if (!isoDateStr)
        return new Date();

    var isoDateRegex = /(\d{4})-?(\d{2})-?(\d{2})([T ](\d{2})(:?(\d{2})(:?(\d{2}(\.\d+)?))?)?(Z|([+-])(\d{2}):?(\d{2})?)?)?/;
    var res = isoDateRegex.exec(isoDateStr);

    if (!res)
        throw "invalid ISO date";

    var year = parseInt(res[1],10) || 1970; // this should always be present
    var month = (parseInt(res[2],10) || 1) - 1;
    var date = parseInt(res[3],10) || 0;
    var hour = parseInt(res[5],10) || 0;
    var min = parseInt(res[7],10) || 0;
    var sec = parseFloat(res[9]) || 0;
    var ms = Math.round((sec%1) * 1000)
    sec -= ms/1000

    var time = Date.UTC(year, month, date, hour, min, sec, ms);

    if (res[11] && res[11] != 'Z'){
        var ofs = 0;
        ofs += (parseInt(res[13],10) || 0) * 60*60*1000; // hours
        ofs += (parseInt(res[14],10) || 0) *    60*1000; // mins
        if (res[12] == '+') // if ahead subtract
            ofs *= -1;

        time += ofs
    }

    return new Date(time);
}

RegExp.escape = function( text ){
    return text.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&");
}

RegExp.prototype.tojson = RegExp.prototype.toString;

Array.contains = function( a  , x ){
    for ( var i=0; i<a.length; i++ ){
        if ( a[i] == x )
            return true;
    }
    return false;
}

Array.unique = function( a ){
    var u = [];
    for ( var i=0; i<a.length; i++){
        var o = a[i];
        if ( ! Array.contains( u , o ) ){
            u.push( o );
        }
    }
    return u;
}

Array.shuffle = function( arr ){
    for ( var i=0; i<arr.length-1; i++ ){
        var pos = i+Random.randInt(arr.length-i);
        var save = arr[i];
        arr[i] = arr[pos];
        arr[pos] = save;
    }
    return arr;
}


Array.tojson = function( a , indent , nolint ){
    var lineEnding = nolint ? " " : "\n";

    if (!indent) 
        indent = "";
    
    if ( nolint )
        indent = "";

    if (a.length == 0) {
        return "[ ]";
    }

    var s = "[" + lineEnding;
    indent += "\t";
    for ( var i=0; i<a.length; i++){
        s += indent + tojson( a[i], indent , nolint );
        if ( i < a.length - 1 ){
            s += "," + lineEnding;
        }
    }
    if ( a.length == 0 ) {
        s += indent;
    }

    indent = indent.substring(1);
    s += lineEnding+indent+"]";
    return s;
}

Array.fetchRefs = function( arr , coll ){
    var n = [];
    for ( var i=0; i<arr.length; i ++){
        var z = arr[i];
        if ( coll && coll != z.getCollection() )
            continue;
        n.push( z.fetch() );
    }
    
    return n;
}

Array.sum = function( arr ){
    if ( arr.length == 0 )
        return null;
    var s = arr[0];
    for ( var i=1; i<arr.length; i++ )
        s += arr[i];
    return s;
}

Array.avg = function( arr ){
    if ( arr.length == 0 )
        return null;
    return Array.sum( arr ) / arr.length;
}

Array.stdDev = function( arr ){
    var avg = Array.avg( arr );
    var sum = 0;

    for ( var i=0; i<arr.length; i++ ){
        sum += Math.pow( arr[i] - avg , 2 );
    }

    return Math.sqrt( sum / arr.length );
}

if( typeof Array.isArray != "function" ){
    Array.isArray = function( arr ){
        return arr != undefined && arr.constructor == Array
    }
}

//these two are helpers for Array.sort(func)
compare = function(l, r){ return (l == r ? 0 : (l < r ? -1 : 1)); }

// arr.sort(compareOn('name'))
compareOn = function(field){
    return function(l, r) { return compare(l[field], r[field]); }
}

Object.keySet = function( o ) {
    var ret = new Array();
    for( var i in o ) {
        if ( !( i in o.__proto__ && o[ i ] === o.__proto__[ i ] ) ) {
            ret.push( i );
        }
    }
    return ret;
}

if ( ! NumberLong.prototype ) {
    NumberLong.prototype = {}
}

NumberLong.prototype.tojson = function() {
    return this.toString();
}

if ( ! NumberInt.prototype ) {
    NumberInt.prototype = {}
}

NumberInt.prototype.tojson = function() {
    return this.toString();
}

if ( ! ObjectId.prototype )
    ObjectId.prototype = {}

ObjectId.prototype.toString = function(){
    return "ObjectId(" + tojson(this.str) + ")";
}

ObjectId.prototype.tojson = function(){
    return this.toString();
}

ObjectId.prototype.valueOf = function(){
    return this.str;
}

ObjectId.prototype.isObjectId = true;

ObjectId.prototype.getTimestamp = function(){
    return new Date(parseInt(this.valueOf().slice(0,8), 16)*1000);
}

ObjectId.prototype.equals = function( other){
    return this.str == other.str;
}

if ( typeof( DBPointer ) != "undefined" ){
    DBPointer.prototype.fetch = function(){
        assert( this.ns , "need a ns" );
        assert( this.id , "need an id" );
        
        return db[ this.ns ].findOne( { _id : this.id } );
    }
    
    DBPointer.prototype.tojson = function(indent){
        return this.toString();
    }

    DBPointer.prototype.getCollection = function(){
        return this.ns;
    }
    
    DBPointer.prototype.getId = function(){
        return this.id;
    }
 
     DBPointer.prototype.toString = function(){
        return "DBPointer(" + tojson(this.ns) + ", " + tojson(this.id) + ")";
    }
}
else {
    //print( "warning: no DBPointer" );
}

if ( typeof( DBRef ) != "undefined" ){
    DBRef.prototype.fetch = function(){
        assert( this.$ref , "need a ns" );
        assert( this.$id , "need an id" );
        
        return db[ this.$ref ].findOne( { _id : this.$id } );
    }
    
    DBRef.prototype.tojson = function(indent){
        return this.toString();
    }

    DBRef.prototype.getCollection = function(){
        return this.$ref;
    }
    
    DBRef.prototype.getRef = function(){
        return this.$ref;
    }

    DBRef.prototype.getId = function(){
        return this.$id;
    }

    DBRef.prototype.toString = function(){
        return "DBRef(" + tojson(this.$ref) + ", " + tojson(this.$id) + ")";
    }
}
else {
    //print( "warning: no DBRef" );
}

if ( typeof( Timestamp ) != "undefined" ){
    Timestamp.prototype.tojson = function () {
        return this.toString();
    }

    Timestamp.prototype.getTime = function () {
        return this.t;
    }

    Timestamp.prototype.getInc = function () {
        return this.i;
    }

    Timestamp.prototype.toString = function () {
        return "Timestamp(" + this.t + ", " + this.i + ")";
    }
}
else {
    //print( "warning: no Timestamp class" );
}

if ( typeof( BinData ) != "undefined" ){
    BinData.prototype.tojson = function () {
        return this.toString();
    }
    
    BinData.prototype.subtype = function () {
        return this.type;
    }
    
    BinData.prototype.length = function () {
        return this.len;
    } 
}
else {
    //print( "warning: no BinData class" );
}

if ( typeof _threadInject != "undefined" ){
    //print( "fork() available!" );
    
    Thread = function(){
        this.init.apply( this, arguments );
    }
    _threadInject( Thread.prototype );
    
    ScopedThread = function() {
        this.init.apply( this, arguments );
    }
    ScopedThread.prototype = new Thread( function() {} );
    _scopedThreadInject( ScopedThread.prototype );
    
    fork = function() {
        var t = new Thread( function() {} );
        Thread.apply( t, arguments );
        return t;
    }    

    // Helper class to generate a list of events which may be executed by a ParallelTester
    EventGenerator = function( me, collectionName, mean, host ) {
        this.mean = mean;
        if (host == undefined) host = db.getMongo().host;
        this.events = new Array( me, collectionName, host );
    }
    
    EventGenerator.prototype._add = function( action ) {
        this.events.push( [ Random.genExp( this.mean ), action ] );
    }
    
    EventGenerator.prototype.addInsert = function( obj ) {
        this._add( "t.insert( " + tojson( obj ) + " )" );
    }

    EventGenerator.prototype.addRemove = function( obj ) {
        this._add( "t.remove( " + tojson( obj ) + " )" );
    }

    EventGenerator.prototype.addUpdate = function( objOld, objNew ) {
        this._add( "t.update( " + tojson( objOld ) + ", " + tojson( objNew ) + " )" );
    }
    
    EventGenerator.prototype.addCheckCount = function( count, query, shouldPrint, checkQuery ) {
        query = query || {};
        shouldPrint = shouldPrint || false;
        checkQuery = checkQuery || false;
        var action = "assert.eq( " + count + ", t.count( " + tojson( query ) + " ) );"
        if ( checkQuery ) {
            action += " assert.eq( " + count + ", t.find( " + tojson( query ) + " ).toArray().length );"
        }
        if ( shouldPrint ) {
            action += " print( me + ' ' + " + count + " );";
        }
        this._add( action );
    }
    
    EventGenerator.prototype.getEvents = function() {
        return this.events;
    }
    
    EventGenerator.dispatch = function() {
        var args = argumentsToArray( arguments );
        var me = args.shift();
        var collectionName = args.shift();
        var host = args.shift();
        var m = new Mongo( host );
        var t = m.getDB( "test" )[ collectionName ];
        for( var i in args ) {
            sleep( args[ i ][ 0 ] );
            eval( args[ i ][ 1 ] );
        }
    }
    
    // Helper class for running tests in parallel.  It assembles a set of tests
    // and then calls assert.parallelests to run them.
    ParallelTester = function() {
        this.params = new Array();
    }
    
    ParallelTester.prototype.add = function( fun, args ) {
        args = args || [];
        args.unshift( fun );
        this.params.push( args );
    }
    
    ParallelTester.prototype.run = function( msg, newScopes ) {
        newScopes = newScopes || false;
        assert.parallelTests( this.params, msg, newScopes );
    }
    
    // creates lists of tests from jstests dir in a format suitable for use by
    // ParallelTester.fileTester.  The lists will be in random order.
    // n: number of lists to split these tests into
    ParallelTester.createJstestsLists = function( n ) {
        var params = new Array();
        for( var i = 0; i < n; ++i ) {
            params.push( [] );
        }

        var makeKeys = function( a ) {
            var ret = {};
            for( var i in a ) {
                ret[ a[ i ] ] = 1;
            }
            return ret;
        }
        
        // some tests can't run in parallel with most others
        var skipTests = makeKeys( [ "jstests/dbadmin.js",
                                   "jstests/repair.js",
                                   "jstests/cursor8.js",
                                   "jstests/recstore.js",
                                   "jstests/extent.js",
                                   "jstests/indexb.js",
                                   "jstests/profile1.js",
                                   "jstests/mr3.js",
                                   "jstests/indexh.js",
                                   "jstests/apitest_db.js",
                                   "jstests/evalb.js",
                                   "jstests/evald.js",
                                   "jstests/evalf.js",
                                   "jstests/killop.js",
                                   "jstests/run_program1.js",
                                   "jstests/notablescan.js",
                                   "jstests/drop2.js",
                                   "jstests/dropdb_race.js",
                                   "jstests/fsync2.js", // May be placed in serialTestsArr once SERVER-4243 is fixed.
                                   "jstests/bench_test1.js",
                                   "jstests/padding.js",
                                   "jstests/queryoptimizera.js",
                                   "jstests/loglong.js" // log might overflow before 
                                                        // this has a chance to see the message
                                  ] );
        
        // some tests can't be run in parallel with each other
        var serialTestsArr = [ "jstests/fsync.js"
//                              ,"jstests/fsync2.js" // SERVER-4243
                              ];
        var serialTests = makeKeys( serialTestsArr );
        
        params[ 0 ] = serialTestsArr;
        
        var files = listFiles("jstests");
        files = Array.shuffle( files );
        
        var i = 0;
        files.forEach(
                      function(x) {
                      
                      if ( ( /[\/\\]_/.test(x.name) ) ||
                          ( ! /\.js$/.test(x.name ) ) ||
                          ( x.name in skipTests ) ||
                          ( x.name in serialTests ) ||
                          ! /\.js$/.test(x.name ) ){ 
                      print(" >>>>>>>>>>>>>>> skipping " + x.name);
                      return;
                      }
                      
                      params[ i % n ].push( x.name );
                      ++i;
                      }
        );
        
        // randomize ordering of the serialTests
        params[ 0 ] = Array.shuffle( params[ 0 ] );
        
        for( var i in params ) {
            params[ i ].unshift( i );
        }
        
        return params;
    }
    
    // runs a set of test files
    // first argument is an identifier for this tester, remaining arguments are file names
    ParallelTester.fileTester = function() {
        var args = argumentsToArray( arguments );
        var suite = args.shift();
        args.forEach(
                     function( x ) {
                     print("         S" + suite + " Test : " + x + " ...");
                     var time = Date.timeFunc( function() { load(x); }, 1);
                     print("         S" + suite + " Test : " + x + " " + time + "ms" );
                     }
                     );        
    }
    
    // params: array of arrays, each element of which consists of a function followed
    // by zero or more arguments to that function.  Each function and its arguments will
    // be called in a separate thread.
    // msg: failure message
    // newScopes: if true, each thread starts in a fresh scope
    assert.parallelTests = function( params, msg, newScopes ) {
        newScopes = newScopes || false;
        var wrapper = function( fun, argv ) {
                   eval (
                         "var z = function() {" +
                         "var __parallelTests__fun = " + fun.toString() + ";" +
                         "var __parallelTests__argv = " + tojson( argv ) + ";" +
                         "var __parallelTests__passed = false;" +
                         "try {" +
                            "__parallelTests__fun.apply( 0, __parallelTests__argv );" +
                            "__parallelTests__passed = true;" +
                         "} catch ( e ) {" +
                            "print( '********** Parallel Test FAILED: ' + tojson(e) );" +
                         "}" +
                         "return __parallelTests__passed;" +
                         "}"
                         );
            return z;
        }
        var runners = new Array();
        for( var i in params ) {
            var param = params[ i ];
            var test = param.shift();
            var t;
            if ( newScopes )
                t = new ScopedThread( wrapper( test, param ) );
            else
                t = new Thread( wrapper( test, param ) );
            runners.push( t );
        }
        
        runners.forEach( function( x ) { x.start(); } );
        var nFailed = 0;
        // v8 doesn't like it if we exit before all threads are joined (SERVER-529)
        runners.forEach( function( x ) { if( !x.returnData() ) { ++nFailed; } } );        
        assert.eq( 0, nFailed, msg );
    }
}

tojsononeline = function( x ){
    return tojson( x , " " , true );
}

tojson = function( x, indent , nolint ){
    if ( x === null )
        return "null";
    
    if ( x === undefined )
        return "undefined";
    
    if (!indent) 
        indent = "";

    switch ( typeof x ) {
    case "string": {
        var s = "\"";
        for ( var i=0; i<x.length; i++ ){
            switch (x[i]){
                case '"': s += '\\"'; break;
                case '\\': s += '\\\\'; break;
                case '\b': s += '\\b'; break;
                case '\f': s += '\\f'; break;
                case '\n': s += '\\n'; break;
                case '\r': s += '\\r'; break;
                case '\t': s += '\\t'; break;

                default: {
                    var code = x.charCodeAt(i);
                    if (code < 0x20){
                        s += (code < 0x10 ? '\\u000' : '\\u00') + code.toString(16);
                    } else {
                        s += x[i];
                    }
                }
            }
        }
        return s + "\"";
    }
    case "number": 
    case "boolean":
        return "" + x;
    case "object":{
        var s = tojsonObject( x, indent , nolint );
        if ( ( nolint == null || nolint == true ) && s.length < 80 && ( indent == null || indent.length == 0 ) ){
            s = s.replace( /[\s\r\n ]+/gm , " " );
        }
        return s;
    }
    case "function":
        return x.toString();
    default:
        throw "tojson can't handle type " + ( typeof x );
    }
    
}

tojsonObject = function( x, indent , nolint ){
    var lineEnding = nolint ? " " : "\n";
    var tabSpace = nolint ? "" : "\t";
    
    assert.eq( ( typeof x ) , "object" , "tojsonObject needs object, not [" + ( typeof x ) + "]" );

    if (!indent) 
        indent = "";
    
    if ( typeof( x.tojson ) == "function" && x.tojson != tojson ) {
        return x.tojson(indent,nolint);
    }
    
    if ( x.constructor && typeof( x.constructor.tojson ) == "function" && x.constructor.tojson != tojson ) {
        return x.constructor.tojson( x, indent , nolint );
    }

    if ( x.toString() == "[object MaxKey]" )
        return "{ $maxKey : 1 }";
    if ( x.toString() == "[object MinKey]" )
        return "{ $minKey : 1 }";
    
    var s = "{" + lineEnding;

    // push one level of indent
    indent += tabSpace;
    
    var total = 0;
    for ( var k in x ) total++;
    if ( total == 0 ) {
        s += indent + lineEnding;
    }

    var keys = x;
    if ( typeof( x._simpleKeys ) == "function" )
        keys = x._simpleKeys();
    var num = 1;
    for ( var k in keys ){
        
        var val = x[k];
        if ( val == DB.prototype || val == DBCollection.prototype )
            continue;

        s += indent + "\"" + k + "\" : " + tojson( val, indent , nolint );
        if (num != total) {
            s += ",";
            num++;
        }
        s += lineEnding;
    }

    // pop one level of indent
    indent = indent.substring(1);
    return s + indent + "}";
}

shellPrint = function( x ){
    it = x;
    if ( x != undefined )
        shellPrintHelper( x );
    
    if ( db ){
        var e = db.getPrevError();
        if ( e.err ) {
            if ( e.nPrev <= 1 )
                print( "error on last call: " + tojson( e.err ) );
            else
                print( "an error " + tojson( e.err ) + " occurred " + e.nPrev + " operations back in the command invocation" );
        }
        db.resetError();
    }
}

printjson = function(x){
    print( tojson( x ) );
}

printjsononeline = function(x){
    print( tojsononeline( x ) );
}

if ( typeof TestData == "undefined" ){
    TestData = undefined
}

jsTestName = function(){
    if( TestData ) return TestData.testName
    return "__unknown_name__"
}

jsTestFile = function(){
    if( TestData ) return TestData.testFile
    return "__unknown_file__"
}

jsTestPath = function(){
    if( TestData ) return TestData.testPath
    return "__unknown_path__"
}

jsTestOptions = function(){
    if( TestData ) return { noJournal : TestData.noJournal,
                            noJournalPrealloc : TestData.noJournalPrealloc,
                            auth : TestData.auth,
                            keyFile : TestData.keyFile,
                            authUser : "__system",
                            authPassword : TestData.keyFileData,
                            adminUser : "admin",
                            adminPassword : "password" }
    return {}
}

jsTestLog = function(msg){
    print( "\n\n----\n" + msg + "\n----\n\n" )
}

jsTest = {}

jsTest.name = jsTestName
jsTest.file = jsTestFile
jsTest.path = jsTestPath
jsTest.options = jsTestOptions
jsTest.log = jsTestLog

jsTest.dir = function(){
    return jsTest.path().replace( /\/[^\/]+$/, "/" )
}

jsTest.randomize = function( seed ) {
    if( seed == undefined ) seed = new Date().getTime()
    Random.srand( seed )
    print( "Random seed for test : " + seed ) 
}

/**
* Adds a user to the admin DB on the given connection. This is only used for running the test suite
* with authentication enabled.
*/
jsTest.addAuth = function(conn) {
    // Get a connection over localhost so that the first user can be added.
    var localconn = conn;
    if ( localconn.host.indexOf('localhost') != 0 ) {
        print( 'Getting locahost connection instead of ' + conn + ' to add first admin user' );
        var hosts = conn.host.split(',');
        for ( var i = 0; i < hosts.length; i++ ) {
            hosts[i] = 'localhost:' + hosts[i].split(':')[1];
        }
        localconn = new Mongo(hosts.join(','));
    }
    print ("Adding admin user on connection: " + localconn);
    return localconn.getDB('admin').addUser(jsTestOptions().adminUser, jsTestOptions().adminPassword,
                                            false, 'majority', 60000);
}

jsTest.authenticate = function(conn) {
    // Set authenticated to stop an infinite recursion from getDB calling back into authenticate
    conn.authenticated = true;
    if (jsTest.options().auth || jsTest.options().keyFile) {
        print ("Authenticating to admin user on connection: " + conn);
        conn.authenticated = conn.getDB('admin').auth(jsTestOptions().adminUser,
                                                      jsTestOptions().adminPassword);
        return conn.authenticated;
    }
}

jsTest.authenticateNodes = function(nodes) {
    jsTest.attempt({timeout:30000, desc: "Authenticate to nodes: " + nodes}, function() {
        for (var i = 0; i < nodes.length; i++) {
            // Don't try to authenticate to arbiters
            res = nodes[i].getDB("admin").runCommand({replSetGetStatus: 1});
            if(res.myState == 7) {
                continue;
            }
            if(jsTest.authenticate(nodes[i]) != 1) {
                return false;
            }
        }
        return true;
    });
}

jsTest.isMongos = function(conn) {
    return conn.getDB('admin').isMaster().msg=='isdbgrid';
}

// Pass this method a function to call repeatedly until
// that function returns true. Example:
//   attempt({timeout: 20000, desc: "get master"}, function() { // return false until success })
jsTest.attempt = function( opts, func ) {
    var timeout = opts.timeout || 1000;
    var tries   = 0;
    var sleepTime = 2000;
    var result = null;
    var context = opts.context || this;

    while((result = func.apply(context)) == false) {
        tries += 1;
        sleep(sleepTime);
        if( tries * sleepTime > timeout) {
            throw('[' + opts['desc'] + ']' + " timed out after " + timeout + "ms ( " + tries + " tries )");
        }
    }

    return result;
}

replSetMemberStatePrompt = function() {
    var state = '';
    var stateInfo = db.getSiblingDB( 'admin' ).runCommand( { replSetGetStatus:1, forShell:1 } );
    if ( stateInfo.ok ) {
        // Report the self member's stateStr if it's present.
        stateInfo.members.forEach( function( member ) {
                                      if ( member.self ) {
                                          state = member.stateStr;
                                      }
                                  } );
        // Otherwise fall back to reporting the numeric myState field (mongodb 1.6).
        if ( !state ) {
            state = stateInfo.myState;
        }
        state = '' + stateInfo.set + ':' + state;
    }
    else {
        var info = stateInfo.info;
        if ( info && info.length < 20 ) {
            state = info; // "mongos", "configsvr"
        }
    }
    return state + '> ';
}

shellPrintHelper = function (x) {
    if (typeof (x) == "undefined") {
        // Make sure that we have a db var before we use it
        // TODO: This implicit calling of GLE can cause subtle, hard to track issues - remove?
        if (__callLastError && typeof( db ) != "undefined" && db.getMongo ) {
            __callLastError = false;
            // explicit w:1 so that replset getLastErrorDefaults aren't used here which would be bad.
            var err = db.getLastError(1);
            if (err != null) {
                print(err);
            }
        }
        return;
    }

    if (x == __magicNoPrint)
        return;

    if (x == null) {
        print("null");
        return;
    }

    if (typeof x != "object")
        return print(x);

    var p = x.shellPrint;
    if (typeof p == "function")
        return x.shellPrint();

    var p = x.tojson;
    if (typeof p == "function")
        print(x.tojson());
    else
        print(tojson(x));
}


shellHelper = function( command , rest , shouldPrint ){
    command = command.trim();
    var args = rest.trim().replace(/\s*;$/,"").split( "\s+" );
    
    if ( ! shellHelper[command] )
        throw "no command [" + command + "]";
    
    var res = shellHelper[command].apply( null , args );
    if ( shouldPrint ){
        shellPrintHelper( res );
    }
    return res;
}

shellHelper.use = function (dbname) {
    var s = "" + dbname;
    if (s == "") {
        print("bad use parameter");
        return;
    }
    db = db.getMongo().getDB(dbname);
    print("switched to db " + db.getName());
}

shellHelper.set = function (str) {
    if (str == "") {
        print("bad use parameter");
        return;
    }
    tokens = str.split(" ");
    param = tokens[0];
    value = tokens[1];
    
    if ( value == undefined ) value = true;
    // value comes in as a string..
    if ( value == "true" ) value = true;
    if ( value == "false" ) value = false;

    if (param == "verbose") {
        _verboseShell = value;
    }
    print("set " + param + " to " + value);
}

shellHelper.it = function(){
    if ( typeof( ___it___ ) == "undefined" || ___it___ == null ){
        print( "no cursor" );
        return;
    }
    shellPrintHelper( ___it___ );
}

shellHelper.show = function (what) {
    assert(typeof what == "string");

    var args = what.split( /\s+/ );
    what = args[0]
    args = args.splice(1)

    if (what == "profile") {
        if (db.system.profile.count() == 0) {
            print("db.system.profile is empty");
            print("Use db.setProfilingLevel(2) will enable profiling");
            print("Use db.system.profile.find() to show raw profile entries");
        }
        else {
            print();
            db.system.profile.find({ millis: { $gt: 0} }).sort({ $natural: -1 }).limit(5).forEach(
                function (x) { 
                    print("" + x.op + "\t" + x.ns + " " + x.millis + "ms " + String(x.ts).substring(0, 24)); 
                    var l = "";
                    for ( var z in x ){
                        if ( z == "op" || z == "ns" || z == "millis" || z == "ts" )
                            continue;
                        
                        var val = x[z];
                        var mytype = typeof(val);
                        
                        if ( mytype == "string" || 
                             mytype == "number" )
                            l += z + ":" + val + " ";
                        else if ( mytype == "object" ) 
                            l += z + ":" + tojson(val ) + " ";
                        else if ( mytype == "boolean" )
                            l += z + " ";
                        else
                            l += z + ":" + val + " ";

                    }
                    print( l );
                    print("\n"); 
                }
            )
        }
        return "";
    }

    if (what == "users") {
        db.system.users.find().forEach(printjson);
        return "";
    }

    if (what == "collections" || what == "tables") {
        db.getCollectionNames().forEach(function (x) { print(x) });
        return "";
    }

    if (what == "dbs") {
        var dbs = db.getMongo().getDBs();
        var size = {};
        dbs.databases.forEach(function (x) { size[x.name] = x.sizeOnDisk; });
        var names = dbs.databases.map(function (z) { return z.name; }).sort();
        names.forEach(function (n) {
            if (size[n] > 1) {
                print(n + "\t" + size[n] / 1024 / 1024 / 1024 + "GB");
            } else {
                print(n + "\t(empty)");
            }
        });
        //db.getMongo().getDBNames().sort().forEach(function (x) { print(x) });
        return "";
    }
    
    if (what == "log" ) {
        var n = "global";
        if ( args.length > 0 )
            n = args[0]
        
        var res = db.adminCommand( { getLog : n } )
        for ( var i=0; i<res.log.length; i++){
            print( res.log[i] )
        }
        return ""
    }

    if (what == "logs" ) {
        var res = db.adminCommand( { getLog : "*" } )
        for ( var i=0; i<res.names.length; i++){
            print( res.names[i] )
        }
        return ""
    }


    throw "don't know how to show [" + what + "]";

}

if ( typeof( Map ) == "undefined" ){
    Map = function(){
        this._data = {};
    }
}

Map.hash = function( val ){
    if ( ! val )
        return val;

    switch ( typeof( val ) ){
    case 'string':
    case 'number':
    case 'date':
        return val.toString();
    case 'object':
    case 'array':
        var s = "";
        for ( var k in val ){
            s += k + val[k];
        }
        return s;
    }

    throw "can't hash : " + typeof( val );
}

Map.prototype.put = function( key , value ){
    var o = this._get( key );
    var old = o.value;
    o.value = value;
    return old;
}

Map.prototype.get = function( key ){
    return this._get( key ).value;
}

Map.prototype._get = function( key ){
    var h = Map.hash( key );
    var a = this._data[h];
    if ( ! a ){
        a = [];
        this._data[h] = a;
    }
    
    for ( var i=0; i<a.length; i++ ){
        if ( friendlyEqual( key , a[i].key ) ){
            return a[i];
        }
    }
    var o = { key : key , value : null };
    a.push( o );
    return o;
}

Map.prototype.values = function(){
    var all = [];
    for ( var k in this._data ){
        this._data[k].forEach( function(z){ all.push( z.value ); } );
    }
    return all;
}

if ( typeof( gc ) == "undefined" ){
    gc = function(){
        print( "warning: using noop gc()" );
    }
}
   

Math.sigFig = function( x , N ){
    if ( ! N ){
        N = 3;
    }
    var p = Math.pow( 10, N - Math.ceil( Math.log( Math.abs(x) ) / Math.log( 10 )) );
    return Math.round(x*p)/p;
}

Random = function() {}

// set random seed
Random.srand = function( s ) { _srand( s ); }

// random number 0 <= r < 1
Random.rand = function() { return _rand(); }

// random integer 0 <= r < n
Random.randInt = function( n ) { return Math.floor( Random.rand() * n ); }

Random.setRandomSeed = function( s ) {
    s = s || new Date().getTime();
    print( "setting random seed: " + s );
    Random.srand( s );
}

// generate a random value from the exponential distribution with the specified mean
Random.genExp = function( mean ) {
    return -Math.log( Random.rand() ) * mean;
}

Geo = {};
Geo.distance = function( a , b ){
    var ax = null;
    var ay = null;
    var bx = null;
    var by = null;
    
    for ( var key in a ){
        if ( ax == null )
            ax = a[key];
        else if ( ay == null )
            ay = a[key];
    }
    
    for ( var key in b ){
        if ( bx == null )
            bx = b[key];
        else if ( by == null )
            by = b[key];
    }

    return Math.sqrt( Math.pow( by - ay , 2 ) + 
                      Math.pow( bx - ax , 2 ) );
}

Geo.sphereDistance = function( a , b ){
    var ax = null;
    var ay = null;
    var bx = null;
    var by = null;
    
    // TODO swap order of x and y when done on server
    for ( var key in a ){
        if ( ax == null )
            ax = a[key] * (Math.PI/180);
        else if ( ay == null )
            ay = a[key] * (Math.PI/180);
    }
    
    for ( var key in b ){
        if ( bx == null )
            bx = b[key] * (Math.PI/180);
        else if ( by == null )
            by = b[key] * (Math.PI/180);
    }

    var sin_x1=Math.sin(ax), cos_x1=Math.cos(ax);
    var sin_y1=Math.sin(ay), cos_y1=Math.cos(ay);
    var sin_x2=Math.sin(bx), cos_x2=Math.cos(bx);
    var sin_y2=Math.sin(by), cos_y2=Math.cos(by);

    var cross_prod = 
        (cos_y1*cos_x1 * cos_y2*cos_x2) +
        (cos_y1*sin_x1 * cos_y2*sin_x2) +
        (sin_y1        * sin_y2);

    if (cross_prod >= 1 || cross_prod <= -1){
        // fun with floats
        assert( Math.abs(cross_prod)-1 < 1e-6 );
        return cross_prod > 0 ? 0 : Math.PI;
    }

    return Math.acos(cross_prod);
}

rs = function () { return "try rs.help()"; }

rs.help = function () {
    print("\trs.status()                     { replSetGetStatus : 1 } checks repl set status");
    print("\trs.initiate()                   { replSetInitiate : null } initiates set with default settings");
    print("\trs.initiate(cfg)                { replSetInitiate : cfg } initiates set with configuration cfg");
    print("\trs.conf()                       get the current configuration object from local.system.replset");
    print("\trs.reconfig(cfg)                updates the configuration of a running replica set with cfg (disconnects)");
    print("\trs.add(hostportstr)             add a new member to the set with default attributes (disconnects)");
    print("\trs.add(membercfgobj)            add a new member to the set with extra attributes (disconnects)");
    print("\trs.addArb(hostportstr)          add a new member which is arbiterOnly:true (disconnects)");
    print("\trs.stepDown([secs])             step down as primary (momentarily) (disconnects)");
    print("\trs.syncFrom(hostportstr)        make a secondary to sync from the given member");
    print("\trs.freeze(secs)                 make a node ineligible to become primary for the time specified");
    print("\trs.remove(hostportstr)          remove a host from the replica set (disconnects)");
    print("\trs.slaveOk()                    shorthand for db.getMongo().setSlaveOk()");
    print();
    print("\tdb.isMaster()                   check who is primary");
    print();
    print("\treconfiguration helpers disconnect from the database so the shell will display");
    print("\tan error, even if the command succeeds.");
    print("\tsee also http://<mongod_host>:28017/_replSet for additional diagnostic info");
}
rs.slaveOk = function (value) { return db.getMongo().setSlaveOk(value); }
rs.status = function () { return db._adminCommand("replSetGetStatus"); }
rs.isMaster = function () { return db.isMaster(); }
rs.initiate = function (c) { return db._adminCommand({ replSetInitiate: c }); }
rs._runCmd = function (c) {
    // after the command, catch the disconnect and reconnect if necessary
    var res = null;
    try {
        res = db.adminCommand(c);
    }
    catch (e) {
        if (("" + e).indexOf("error doing query") >= 0) {
            // closed connection.  reconnect.
            db.getLastErrorObj();
            var o = db.getLastErrorObj();
            if (o.ok) {
                print("reconnected to server after rs command (which is normal)");
            }
            else {
                printjson(o);
            }
        }
        else {
            print("shell got exception during repl set operation: " + e);
            print("in some circumstances, the primary steps down and closes connections on a reconfig");
        }
        return "";
    }
    return res;
}
rs.reconfig = function (cfg, options) {
    cfg.version = rs.conf().version + 1;
    cmd = { replSetReconfig: cfg };
    for (var i in options) {
        cmd[i] = options[i];
    }
    return this._runCmd(cmd);
}
rs.add = function (hostport, arb) {
    var cfg = hostport;

    var local = db.getSisterDB("local");
    assert(local.system.replset.count() <= 1, "error: local.system.replset has unexpected contents");
    var c = local.system.replset.findOne();
    assert(c, "no config object retrievable from local.system.replset");

    c.version++;

    var max = 0;
    for (var i in c.members)
        if (c.members[i]._id > max) max = c.members[i]._id;
    if (isString(hostport)) {
        cfg = { _id: max + 1, host: hostport };
        if (arb)
            cfg.arbiterOnly = true;
    }
    c.members.push(cfg);
    return this._runCmd({ replSetReconfig: c });
}
rs.syncFrom = function (host) { return db._adminCommand({replSetSyncFrom : host}); };
rs.stepDown = function (secs) { return db._adminCommand({ replSetStepDown:(secs === undefined) ? 60:secs}); }
rs.freeze = function (secs) { return db._adminCommand({replSetFreeze:secs}); }
rs.addArb = function (hn) { return this.add(hn, true); }
rs.conf = function () { return db.getSisterDB("local").system.replset.findOne(); }
rs.config = function () { return rs.conf(); }

rs.remove = function (hn) {
    var local = db.getSisterDB("local");
    assert(local.system.replset.count() <= 1, "error: local.system.replset has unexpected contents");
    var c = local.system.replset.findOne();
    assert(c, "no config object retrievable from local.system.replset");
    c.version++;

    for (var i in c.members) {
        if (c.members[i].host == hn) {
            c.members.splice(i, 1);
            return db._adminCommand({ replSetReconfig : c});
        }
    }

    return "error: couldn't find "+hn+" in "+tojson(c.members);
};

rs.debug = {};

rs.debug.nullLastOpWritten = function(primary, secondary) {
    var p = connect(primary+"/local");
    var s = connect(secondary+"/local");
    s.getMongo().setSlaveOk();

    var secondToLast = s.oplog.rs.find().sort({$natural : -1}).limit(1).next();
    var last = p.runCommand({findAndModify : "oplog.rs",
                             query : {ts : {$gt : secondToLast.ts}},
                             sort : {$natural : 1},
                             update : {$set : {op : "n"}}});

    if (!last.value.o || !last.value.o._id) {
        print("couldn't find an _id?");
    }
    else {
        last.value.o = {_id : last.value.o._id};
    }

    print("nulling out this op:");
    printjson(last);
};

rs.debug.getLastOpWritten = function(server) {
    var s = db.getSisterDB("local");
    if (server) {
        s = connect(server+"/local");
    }
    s.getMongo().setSlaveOk();

    return s.oplog.rs.find().sort({$natural : -1}).limit(1).next();
};


help = shellHelper.help = function (x) {
    if (x == "mr") {
        print("\nSee also http://dochub.mongodb.org/core/mapreduce");
        print("\nfunction mapf() {");
        print("  // 'this' holds current document to inspect");
        print("  emit(key, value);");
        print("}");
        print("\nfunction reducef(key,value_array) {");
        print("  return reduced_value;");
        print("}");
        print("\ndb.mycollection.mapReduce(mapf, reducef[, options])");
        print("\noptions");
        print("{[query : <query filter object>]");
        print(" [, sort : <sort the query.  useful for optimization>]");
        print(" [, limit : <number of objects to return from collection>]");
        print(" [, out : <output-collection name>]");
        print(" [, keeptemp: <true|false>]");
        print(" [, finalize : <finalizefunction>]");
        print(" [, scope : <object where fields go into javascript global scope >]");
        print(" [, verbose : true]}\n");
        return;
    } else if (x == "connect") {
        print("\nNormally one specifies the server on the mongo shell command line.  Run mongo --help to see those options.");
        print("Additional connections may be opened:\n");
        print("    var x = new Mongo('host[:port]');");
        print("    var mydb = x.getDB('mydb');");
        print("  or");
        print("    var mydb = connect('host[:port]/mydb');");
        print("\nNote: the REPL prompt only auto-reports getLastError() for the shell command line connection.\n");
        return;
    }
    else if (x == "keys") {
        print("Tab completion and command history is available at the command prompt.\n");
        print("Some emacs keystrokes are available too:");
        print("  Ctrl-A start of line");
        print("  Ctrl-E end of line");
        print("  Ctrl-K del to end of line");
        print("\nMulti-line commands");
        print("You can enter a multi line javascript expression.  If parens, braces, etc. are not closed, you will see a new line ");
        print("beginning with '...' characters.  Type the rest of your expression.  Press Ctrl-C to abort the data entry if you");
        print("get stuck.\n");
    }
    else if (x == "misc") {
        print("\tb = new BinData(subtype,base64str)  create a BSON BinData value");
        print("\tb.subtype()                         the BinData subtype (0..255)");
        print("\tb.length()                          length of the BinData data in bytes");
        print("\tb.hex()                             the data as a hex encoded string");
        print("\tb.base64()                          the data as a base 64 encoded string");
        print("\tb.toString()");
        print();
        print("\tb = HexData(subtype,hexstr)         create a BSON BinData value from a hex string");
        print("\tb = UUID(hexstr)                    create a BSON BinData value of UUID subtype");
        print("\tb = MD5(hexstr)                     create a BSON BinData value of MD5 subtype");
        print("\t\"hexstr\"                            string, sequence of hex characters (no 0x prefix)");
        print();
        print("\to = new ObjectId()                  create a new ObjectId");
        print("\to.getTimestamp()                    return timestamp derived from first 32 bits of the OID");
        print("\to.isObjectId()");
        print("\to.toString()");
        print("\to.equals(otherid)");
        print();
        print("\td = ISODate()                       like Date() but behaves more intuitively when used");
        print("\td = ISODate('YYYY-MM-DD hh:mm:ss')    without an explicit \"new \" prefix on construction");
        return;
    }
    else if (x == "admin") {
        print("\tls([path])                      list files");
        print("\tpwd()                           returns current directory");
        print("\tlistFiles([path])               returns file list");
        print("\thostname()                      returns name of this host");
        print("\tcat(fname)                      returns contents of text file as a string");
        print("\tremoveFile(f)                   delete a file or directory");
        print("\tload(jsfilename)                load and execute a .js file");
        print("\trun(program[, args...])         spawn a program and wait for its completion");
        print("\trunProgram(program[, args...])  same as run(), above");
        print("\tsleep(m)                        sleep m milliseconds");
        print("\tgetMemInfo()                    diagnostic");
        return;
    }
    else if (x == "test") {
        print("\tstartMongodEmpty(args)        DELETES DATA DIR and then starts mongod");
        print("\t                              returns a connection to the new server");
        print("\tstartMongodTest(port,dir,options)");
        print("\t                              DELETES DATA DIR");
        print("\t                              automatically picks port #s starting at 27000 and increasing");
        print("\t                              or you can specify the port as the first arg");
        print("\t                              dir is /data/db/<port>/ if not specified as the 2nd arg");
        print("\t                              returns a connection to the new server");
        print("\tresetDbpath(dirpathstr)       deletes everything under the dir specified including subdirs");
        print("\tstopMongoProgram(port[, signal])");
        return;
    }
    else if (x == "") {
        print("\t" + "db.help()                    help on db methods");
        print("\t" + "db.mycoll.help()             help on collection methods");
        print("\t" + "sh.help()                    sharding helpers");
        print("\t" + "rs.help()                    replica set helpers");
        print("\t" + "help admin                   administrative help");
        print("\t" + "help connect                 connecting to a db help");
        print("\t" + "help keys                    key shortcuts");
        print("\t" + "help misc                    misc things to know");
        print("\t" + "help mr                      mapreduce");
        print();
        print("\t" + "show dbs                     show database names");
        print("\t" + "show collections             show collections in current database");
        print("\t" + "show users                   show users in current database");
        print("\t" + "show profile                 show most recent system.profile entries with time >= 1ms");
        print("\t" + "show logs                    show the accessible logger names");
        print("\t" + "show log [name]              prints out the last segment of log in memory, 'global' is default");
        print("\t" + "use <db_name>                set current database");
        print("\t" + "db.foo.find()                list objects in collection foo");
        print("\t" + "db.foo.find( { a : 1 } )     list objects in foo where a == 1");
        print("\t" + "it                           result of the last line evaluated; use to further iterate");
        print("\t" + "DBQuery.shellBatchSize = x   set default number of items to display on shell");
        print("\t" + "exit                         quit the mongo shell");
    }
    else
        print("unknown help option");
}
