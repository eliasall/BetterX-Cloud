
App.prototype.removeMapMarkers = function() {
  this.mapMarkers.forEach(function(marker) {
    marker.setMap(null);
  })
  this.mapMarkers = [];

  this.labels.forEach(function(label) {
    label.setMap(null);
  })
  this.labels = [];
}

App.prototype.startMap = function() {

  this.mode = "map";

  var selection = this.getFormData(true);

  if (selection) {

    var self = this;

    $.ajax("data/" + selection.user + "_" + this.mapFileTypeName + "_" + selection.day + ".csv", {
      complete: function(jqXHR, textStatus) {
        switch (jqXHR.status) {
          case 200:
            self.removeGraphDivs();
            if (self.map) {
              self.showMap();
            } else {
              self.addMapWidget();
            }
            break;
          default:
            notie.alert(3, "locations not found", 1);
        }
      }
    })
  }
}

App.prototype.showMap = function() {
  this.removeMapMarkers();
  this.addMapMarkers();
}

App.prototype.hideMap = function() {
  this.removeMapMarkers();
  $("#map").hide();
}

App.prototype.addMapWidget = function() {

  this.startLoadingAnimation();

  $("#results").after($('<script src="https://maps.googleapis.com/maps/api/js?key=<INSERTKEYHERE>&callback=app.initMap"></script>'));
}

App.prototype.initMap = function() {

  $("#map").css("height", "600px");
  $("#map").show();

  this.map = new google.maps.Map(document.getElementById("map"));

  $("#results").after($('<script src="lib/maplabel.js"></script>'));

  this.infoWindow = new google.maps.InfoWindow();

  this.addMapMarkers();

  this.stopLoadingAnimation();
}

App.prototype.addMapMarkers = function() {

  var selectedUser = $("#user_id option:selected").val()
  var selectedDay = $("#day option:selected").val();

  var bounds = new google.maps.LatLngBounds();

  var self = this;

  d3.csv("data/" + selectedUser + "_" + this.mapFileTypeName + "_" + selectedDay + ".csv", function(data) {

    var minMax = self.getMinMaxDates();
	console.log(minMax);

    var sortedData = data.filter(function(d) {
      return (d.timestamp * 1000) >= minMax.min && (d.timestamp * 1000) <= minMax.max;  //edit here * 1000
    })
	
	console.log(sortedData);

    if (sortedData.length == 0) {
  
      var minData = data.filter(function(d) {
        return d.timestamp < minMax.min;
      })

      var maxMinDate = minData.sort(function(a, b) { return d3.descending(a.timestamp, b.timestamp); }) [0];
      sortedData.push(maxMinDate);
    }

    $("#map").show();

    sortedData.forEach(function(value, key) {

      var position = new google.maps.LatLng(value.lat, value.lon);

      var marker = new google.maps.Marker({
        position: position,
        title: String(key + 1),
        map: self.map
      })

      marker.addListener("click", function() {
        self.infoWindow.setContent(value.desc);
        self.infoWindow.open(self.map, marker);
      })

      self.mapMarkers.push(marker);

      var label = new MapLabel({
        text: String(key + 1),
        position: position,
        map: self.map,
        fontSize: 12,
        align: 'left'
      })

      self.labels.push(label);

      bounds.extend(position);

      self.map.fitBounds(bounds);
    })

  })
}