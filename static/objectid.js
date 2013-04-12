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
