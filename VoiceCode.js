console.log("hello CRC16");

  function CreateCrc16(){
      var polynomial,value,temp;
      var table=new Array(256);
      
        polynomial=40961;
      for(i=0;i<table.length;++i){
        value=0;temp=i;
      for(j=0;j<8;++j){if(0!=((value^temp)&1)){
        value=(value>>1)^polynomial
      }else{
        value>>=1
      }
        temp>>=1
      }
        table[i]=value
      }return table
    }
    
    var crc,table,testString,index;
    var GTIN = "17802821008322";
    var Lot = "890563247"
    var date = "220711"
    testString = "17802821008322890563247220711";
    


    console.log("Plain Text = " +testString);
    
    /*if(testString != ""){
      testString=GTIN+Lot+date;
    }*/
    
    crc=0;
    table=CreateCrc16();
    
    for(i=0;i<testString.length;++i){
      index=(crc^testString.charCodeAt(i))%256;
      crc=(crc>>8)^table[index]
    }
    
    console.log("crc = " +crc);
    
    var vc=(crc%10000)+"";
    var length=vc.length;
    for(i=vc.length;i<4;i++)
    {
      vc="0"+vc
    }

    console.log("Small Digits" + vc.substr(0,2));
    console.log("Large Digits" + vc.substr(2,2));