function doGet(req) {
  var dateReq = req.parameter.date;
  var pathLast = req.pathInfo;
  var doc = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = doc.getSheetByName("DATA");
  var values = sheet.getDataRange().getValues();

  var rows = values.slice(1).map(getRow);

  if(pathLast === "last") {
    return ContentService.createTextOutput(JSON.stringify({data: rows[rows.length - 1]})).setMimeType(ContentService.MimeType.JSON);
  }

  if (dateReq != null) {
    rows = rows.filter(obj => obj.date == dateReq);
  }
  return ContentService.createTextOutput(JSON.stringify({data: rows})).setMimeType(ContentService.MimeType.JSON);
}

function getRow(values) {
  var row = {};
  row['date'] = Utilities.formatDate(new Date(values[0]), "GMT+2", "yyyy-MM-dd");
  row['time'] = values[1];
  row['temperature'] = values[2];
  row['humidity'] = values[3];
  row['temperature_pero'] = values[4];
  row['humidity_pero'] = values[5];
  row['weather_description'] = values[6];
  return row;
}