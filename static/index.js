/*function update(){
    val = document.getElementById("User-Input").value;
    document.getElementsByClassName("prediction")[0].innerHTML = "";
}
*/

function update() {
    $.getJSON('/price', function(data) {
      $('#New-price').text(data.price);
      $('#Date-and-Time').text(data.datetime);
      $('#New-price').text(data.price);
      $('#low-price-id').text(data.low_price);
      $('#high-price-id').text(data.high_price);
      $('#range-datetime-id').text(data.datetime);
      $('#range-volume-id').text(data.baseVolume);
      $('#predicted-volume-id').text(data.baseVolume);
      $('#change-id').text(data.change);
      $('#current-volume-id').text(data.baseVolume);
      $('#Open-price-id').text(data.Open1);
      $('#Close-price-id').text(data.close1);
      $('#average-price-id').text(data.average);
      $('#previousClose-id').text(data.previousClose);
      $('#predicted-price-id').text(data.future_price);
      $('#predicted-datetime-id').text(data.future_date);
    });
  };
  setInterval(update, 1000);
  



  $(document).ready(function(){
    $('form').submit(function(event){
      event.preventDefault();
      $.ajax({
        type:'POST',
        url:'/predict_it',
        data:$('form').serialize(), 
        success: function(data){
          $('#prediction-id').text(data.response);
        }
      });
    });    
  });