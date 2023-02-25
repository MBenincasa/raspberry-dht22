function doGet(req) {
  var doc = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = doc.getSheetByName("DATA");
  var values = sheet.getDataRange().getValues();

  var output = []
  for (var i = 1; i < values.length; i++) {
    var row = {};
    row['date'] = values[i][0];
    row['time'] = values[i][1];
    row['temperature'] = values[i][2];
    row['humidity'] = values[i][3];
    output.push(row);
  }

  return ContentService.createTextOutput(JSON.stringify({data: output})).setMimeType(ContentService.MimeType.JSON);
}
