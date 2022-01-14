var ss = SpreadsheetApp.getActiveSpreadsheet();
var sheet = ss.getActiveSheet();
var dataRangeAll = sheet.getDataRange();
var ultimaFila = dataRangeAll.getLastRow();

function onOpen() {  
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('Geocodificar')
      .addItem('Convertir direcciones en coordenadas', 'geocodificar')
      .addToUi();
}

// Geocodificar todas las filas a partir de la 2da
function geocodificar() {
  var filaInicial = 2;
  var dataRange = sheet.getRange(filaInicial, 1,ultimaFila, 3);
  var data = dataRange.getValues();

  // Recorremos todas las filas del rango
  for(var i=0; i< data.length; i++) {
    var fila = data[i];

    var direccion = fila[0];

    // Solamente haremos algo si hay algo en la celda de dirección, 
    // para no generar errores inesperados
    if(direccion != "") {
      // Aquí es donde nos conectamos con Google Maps
      var geocoder = Maps.newGeocoder().geocode(direccion);
      var resultado = geocoder.results[0];

      var latitud = 0;
      var longitud = 0;

      // Si el geocoder de Google Maps nos devuelve un resultado satisfactorio, 
      // escribimos la latitud y longitud en las celdas correspondientes al a fila
      if(resultado) {
        latitud = resultado.geometry.location.lat;
        longitud = resultado.geometry.location.lng;
        sheet.getRange(filaInicial + i, 2).setValue(latitud);
        sheet.getRange(filaInicial + i, 3).setValue(longitud);
      }
    }
  }
}