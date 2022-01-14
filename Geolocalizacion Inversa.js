var ss = SpreadsheetApp.getActiveSpreadsheet();
var sheet = ss.getActiveSheet();
var dataRangeAll = sheet.getDataRange();
var ultimaFila = dataRangeAll.getLastRow();

function onOpen() {  
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('Geocodificación')
      .addItem('Convertir coordenadas en direcciones', 'geocodificacion_inversa')
      .addToUi();
}

// Geocodificar inversamente todas las filas a partir de la 2da
function geocodificacion_inversa() {
  var filaInicial = 2;
  var dataRange = sheet.getRange(filaInicial, 1,ultimaFila, 3);
  var data = dataRange.getValues();

  // Recorremos todas las filas del rango
  for(var i=0; i< data.length; i++) {
    var fila = data[i];

    var latitud = fila[0];
    var longitud = fila[1];

    // Solamente haremos algo si no están vacías las coordenadas, 
    // para no generar errores inesperados
    if(latitud != "" && longitud != "") {
      // Aquí es donde nos conectamos con Google Maps
      var reverseGeocoder = Maps.newGeocoder().reverseGeocode(latitud, longitud);
      var resultado = reverseGeocoder.results[0];

      var direccion = "";

      // Si el geocoder de Google Maps nos devuelve un resultado satisfactorio, 
      // escribimos la dirección en las celdas correspondientes a la fila
      if(resultado) {
        direccion = resultado.formatted_address;
        sheet.getRange(filaInicial + i, 3).setValue(direccion);
      }
    }
  }
}