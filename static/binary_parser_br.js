var chr = String.fromCharCode;

var maxBits = [];
for (var i = 0; i < 64; i++) {
	maxBits[i] = Math.pow(2, i);
}

function BinaryParser (bigEndian, allowExceptions) {
  if(!(this instanceof BinaryParser)) return new BinaryParser(bigEndian, allowExceptions);
  
	this.bigEndian = bigEndian;
	this.allowExceptions = allowExceptions;
};

BinaryParser.encodeInt = function encodeInt (data, bits, signed, forceBigEndian) {
	var max = maxBits[bits];

  if (data >= max || data < -(max / 2)) {
    //log.warn("encodeInt::overflow");
    data = 0;
  }

	if (data < 0) {
    data += max;
  }

	for (var r = []; data; r[r.length] = String.fromCharCode(data % 256), data = Math.floor(data / 256));

	for (bits = -(-bits >> 3) - r.length; bits--; r[r.length] = "\0");

  return ((this.bigEndian || forceBigEndian) ? r.reverse() : r).join("");
};

BinaryParser.fromSmall  = function( data ){ return this.encodeInt( data,  8, true  ); };
BinaryParser.fromByte   = function( data ){ return this.encodeInt( data,  8, false ); };
BinaryParser.fromShort  = function( data ){ return this.encodeInt( data, 16, true  ); };
BinaryParser.fromWord   = function( data ){ return this.encodeInt( data, 16, false ); };
BinaryParser.fromInt    = function( data ){ return this.encodeInt( data, 32, true  ); };
BinaryParser.fromLong   = function( data ){ return this.encodeInt( data, 64, true  ); };
BinaryParser.fromDWord  = function( data ){ return this.encodeInt( data, 32, false ); };
BinaryParser.fromQWord  = function( data ){ return this.encodeInt( data, 64, true ); };
BinaryParser.fromFloat  = function( data ){ return this.encodeFloat( data, 23, 8   ); };
BinaryParser.fromDouble = function( data ){ return this.encodeFloat( data, 52, 11  ); };
