DBQuery = {}
DBQuery.shellBatchSize = 20;
var print = function(args){
    jqconsole.Write(args.toString() + '\n', 'jqconsole-output');
}


function DB(){ }
DB.notimplemented = function(){
    print("This function is not implemented in the browser shell.");
}
DB.prototype.help = function(){
    print("TODO: print help information for each command.");
}
DB.prototype.getCollection = function(){
}
DB.prototype.getCollectionNames = function(){}         
DB.prototype.getLastError = function(){}               
DB.prototype.getLastErrorCmd = function(){}            
DB.prototype.getLastErrorObj = function(){}            
DB.prototype.getMongo = notimplemented                   
DB.prototype.getName = function(){ return this.name }                    
DB.prototype.getPrevError = function(){}               
DB.prototype.getProfilingLevel = function(){ return 0; }          
DB.prototype.getProfilingStatus = notimplemented;
DB.prototype.getReplicationInfo = notimplemented;         
DB.prototype.getSiblingDB = notimplemented;               
DB.prototype.getSisterDB = notimplemented;                
DB.prototype.getSlaveOk = notimplemented;                 
DB.prototype.group = notimplemented;                      
DB.prototype.groupcmd = notimplemented;                   
DB.prototype.groupeval = notimplemented;                  
DB.prototype.hostInfo = function (){ return this._adminCommand( "hostInfo" ); }
DB.prototype.isMaster = function () { return this.runCommand("isMaster"); }
DB.prototype.killOP = function(){}(                     
DB.prototype.killOp = function(){}(                     

DB.prototype.listCommands = //{{{
    function (){
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
    }//}}}

DB.prototype.loadServerScripts = notimplemented;          
DB.prototype.logout = notimplemented;
DB.prototype.printCollectionStats = //{{{
    function (scale){
        /* no error checking on scale, done in stats already */
        var mydb = this;
        this.getCollectionNames().forEach(
            function(z){
                print( z );
                printjson( mydb.getCollection(z).stats(scale) );
                print( "---" );
             }
        );
    }//}}}
DB.prototype.printReplicationInfo = notimplemented
DB.prototype.printShardingStatus = notimplemented        
DB.prototype.printSlaveReplicationInfo = notimplemented  
DB.prototype.removeUser = notimplemented                 
DB.prototype.repairDatabase = notimplemented             
DB.prototype.resetError = notimplemented                 
DB.prototype.runCommand = notimplemented                 
DB.prototype.serverBits = function (){ return this.serverBuildInfo().bits; }
DB.prototype.serverBuildInfo = function (){ return this._adminCommand( "buildinfo" ); }
DB.prototype.serverCmdLineOpts = function (){ return this._adminCommand( "getCmdLineOpts" ); }
DB.prototype.serverStatus = function (){ return this._adminCommand( "serverStatus" ); }
DB.prototype.setProfilingLevel =//{{{
    function (level,slowms) {
        if (level < 0 || level > 2) {
            throw { dbSetProfilingException :
                    "input level " + level + " is out of range [0..2]" };
        }
        var cmd = { profile: level };
        if ( slowms )
            cmd["slowms"] = slowms;
        return this._dbCommand( cmd );
    }//}}}
DB.prototype.setSlaveOk = notimplemented;                                       
DB.prototype.shutdownServer = notimplemented;                                   
DB.prototype.stats = function (scale){ return this.runCommand( { dbstats : 1 , scale : scale } ); }                                            
DB.prototype.toString = function(){ return this.name; }           
DB.prototype.tojson = function(){}(             
DB.prototype.treasury_input = function(){}      
DB.prototype.valueOf = function(){}(            
DB.prototype.version = function(){}(            
DB.prototype.base64 = function(){}                      

var DBCollection = function(){}
DBCollection.prototype.aggregate(              
DBCollection.prototype.base64                  
DBCollection.prototype.clean(                  
DBCollection.prototype.constructor             
DBCollection.prototype.convertToCapped(        
DBCollection.prototype.convertToSingleObject(  
DBCollection.prototype.count(                  
DBCollection.prototype.createIndex(            
DBCollection.prototype.dataSize(               
DBCollection.prototype.diskStorageStats(       
DBCollection.prototype.distinct(               
DBCollection.prototype.drop(                   
DBCollection.prototype.dropIndex(              
DBCollection.prototype.dropIndexes(            
DBCollection.prototype.ensureIndex(            
DBCollection.prototype.exists(                 
DBCollection.prototype.find(                   
DBCollection.prototype.findAndModify(          
DBCollection.prototype.findOne(                
DBCollection.prototype.gavg(                   
DBCollection.prototype.gcount(                 
DBCollection.prototype.getCollection(          
DBCollection.prototype.getDB(                  
DBCollection.prototype.getDiskStorageStats(    
DBCollection.prototype.getFullName(            
DBCollection.prototype.getIndexKeys(           
DBCollection.prototype.getIndexSpecs(          
DBCollection.prototype.getIndexStats(          
DBCollection.prototype.getIndexes(             
DBCollection.prototype.getIndices(             
DBCollection.prototype.getMongo(               
DBCollection.prototype.getName(                
DBCollection.prototype.getPagesInRAM(          
DBCollection.prototype.getQueryOptions(        
DBCollection.prototype.getShardDistribution(   
DBCollection.prototype.getShardVersion(        
DBCollection.prototype.getSlaveOk(             
DBCollection.prototype.getSplitKeysForChunks(  
DBCollection.prototype.group(                  
DBCollection.prototype.groupcmd(               
DBCollection.prototype.gsum(                   
DBCollection.prototype.hasOwnProperty(         
DBCollection.prototype.help(                   
DBCollection.prototype.hex                     
DBCollection.prototype.indexStats(             
DBCollection.prototype.insert(                 
DBCollection.prototype.isCapped(               
DBCollection.prototype.length                  
DBCollection.prototype.mapReduce(              
DBCollection.prototype.pagesInRAM(             
DBCollection.prototype.propertyIsEnumerable(       
DBCollection.prototype.prototype                   
DBCollection.prototype.reIndex(                    
DBCollection.prototype.remove(                     
DBCollection.prototype.renameCollection(           
DBCollection.prototype.runCommand(                 
DBCollection.prototype.save(                       
DBCollection.prototype.setSlaveOk(                 
DBCollection.prototype.shellPrint(                                        
DBCollection.prototype.stats(                                             
DBCollection.prototype.storageSize(                                       
DBCollection.prototype.subtype                                            
DBCollection.prototype.toLocaleString(                                    
DBCollection.prototype.toString(      
DBCollection.prototype.tojson(        
DBCollection.prototype.totalIndexSize(
DBCollection.prototype.totalSize(     
DBCollection.prototype.update(        
DBCollection.prototype.validate(      
DBCollection.prototype.valueOf(       
DBCollection.prototype.verify(        
