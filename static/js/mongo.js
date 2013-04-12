var print = function(){

    for(var i=0;i<arguments.length;i++){
        jqconsole.Write(arguments[i], 'jqconsole-output');
        if(i != arguments.length - 1){
            jqconsole.Write(' ', 'jqconsole-output');
        }
    }
    jqconsole.Write('\n', 'jqconsole-output');
}
