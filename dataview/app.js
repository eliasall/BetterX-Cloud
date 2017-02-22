
var App = function () {
  this.host = "http://<INSERTFOLDERNAMEHERE>s3-website-us-east-1.amazonaws.com/data/";
  this.mapFileTypeName = "locations";
  this.mapMarkers = [];
  this.labels = [];

  $("#min").datetimepicker({
    format: "HH:mm"
  });

  $("#max").datetimepicker({
    format: "HH:mm"
  });

  $("#files").multiselect({
    includeSelectAllOption: true,
    buttonWidth: "280px",
    buttonClass: "btn",
    numberDisplayed: 2,
    disableIfEmpty: true
  });

  this.mode = "";
}

App.prototype.load = function() {

  var self = this;

  d3.csv("data/index.csv", function(data) {

    self.data = d3.nest()
      .key(function(d) { return d.userid; })
      .key(function(d) { return d.dayofYear; })
      .entries(data);

    self.data.forEach(function(value) {
      $("#user_id")
        .append($("<option></option>")
        .attr("value", value.key)
        .text(value.key));
    })
  })
}

App.prototype.selectUser = function() {

  $("#files").multiselect("dataprovider", []);

  $("#day option").remove();
  $("#day")
    .append($("<option></option>")
    .attr("value", "")
    .text("Select Day"));

  if (!$("#user_id option:selected").val()) {
    return;
  }

  var user = this.data.filter(function(value) { return value.key == $("#user_id option:selected").val() })[0];

  user.values.forEach(function(value) {
    $("#day")
      .append($("<option></option>")
      .attr("value", value.key)
      .text(value.key));
  })
}

App.prototype.selectDay = function() {
  $("#files").multiselect("dataprovider", []);
  this.populateFiles();
}

App.prototype.populateFiles = function() {

//    $("#files").multiselect("dataprovider", []);

  if (selection = this.getFormData(false)) {

    var user = this.data.filter(function(value) { return value.key == selection.user })[0].values;
    var userRows = user.filter(function(value) { return value.key == selection.day })[0].values;

    var self = this;

    var fileTypeNames = userRows.map(function(row) {
      return {
        value: row.fileTypeName,
        label: row.fileTypeName,
        selected: true
      }
    })

    fileTypeNames = fileTypeNames.filter(function(name) {
      return name.value != self.mapFileTypeName;
    })

    $("#files").multiselect("dataprovider", fileTypeNames);
  }
}

App.prototype.getMinMaxDates = function() {

  var today = new Date();

  var selectedDay = $("#day option:selected").val();

  var minDate = new Date(Date.UTC(today.getFullYear(), 0, selectedDay, 0, 0, 0));

  var maxDate = new Date(Date.UTC(today.getFullYear(), 0, selectedDay, 23, 59, 59));

  if ($("#min").val()) {
    var time = $("#min").val().split(":");
    minDate.setUTCHours(time[0], time[1]);
  }

  if ($("#max").val()) {
    var time = $("#max").val().split(":");
    maxDate.setUTCHours(time[0], time[1]);
  }

  return {
    min: minDate.getTime(), 
    max: maxDate.getTime()
  }
}

App.prototype.readFile = function(filename, fileTypeName) {

  var self = this;

  var deferred = $.Deferred();

  d3.csv(filename, function(data) {

    var minMax = self.getMinMaxDates();

    data = data.map(function(d) {
      d.timestamp = d.timestamp * 1000;
      return d;
    })

    data = data.sort(function(a, b) {
      return a.timestamp - b.timestamp;
    })

    data = data.filter(function(d) {
      return d.timestamp >= minMax.min && d.timestamp <= minMax.max;
    })

    data = data.map(function(d) {
      if (Number(d.value)) { 
        return {
          date: new Date(Number(d.timestamp)),
          value: Number(d.value)
        }
      } else { 
        return {
          date: new Date(Number(d.timestamp)),
          value: 0,
          trueValue: d.value
        }
      }
    })

    deferred.resolve({data: data, fileTypeName: fileTypeName});
  })

  return deferred.promise();
}

App.prototype.getFormData = function(validate) {

  var selectedUser = $("#user_id option:selected").val()
  var selectedDay = $("#day option:selected").val();

  if (!selectedUser) {
    if (validate) notie.alert(3, "Please select UserID", 1);
    return false;
  }

  if (!selectedDay) {
    if (validate) notie.alert(3, "Please select Day", 1);
    return false;
  }

  if (validate && this.mode == "graph" && !$("#files").val()) {
    notie.alert(3, "Please select File(s)", 1);
    return false;
  }

return {
    user: selectedUser, 
    day: selectedDay
  }
}

App.prototype.startLoadingAnimation = function() {
  $("body").waitMe({
    text: "Please wait..."
  });
}

App.prototype.stopLoadingAnimation = function() {
  $("body").waitMe("hide");
}

