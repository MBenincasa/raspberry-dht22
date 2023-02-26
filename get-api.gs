function doGet(req) {
  var dateReq = req.parameter.date;
  var doc = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = doc.getSheetByName("DATA");
  var values = sheet.getDataRange().getValues();

  var output = []
  for (var i = 1; i < values.length; i++) {
    var row = {};
    row['date'] = Utilities.formatDate(new Date(values[i][0]), "GMT+1", "yyyy-MM-dd");
    row['time'] = values[i][1];
    row['temperature'] = values[i][2];
    row['humidity'] = values[i][3];
    output.push(row);
  }

  if (dateReq != null) {
    output = output.filter(obj => obj.date == dateReq);
  }
  return ContentService.createTextOutput(JSON.stringify({data: output})).setMimeType(ContentService.MimeType.JSON);
}
