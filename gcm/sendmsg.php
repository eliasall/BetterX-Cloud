<!DOCTYPE html>

<html>
    <head>
        <title>BetterX Send Message</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min.js"></script>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/s/dt/dt-1.10.10/datatables.min.css"/> 
        <script type="text/javascript" src="https://cdn.datatables.net/s/dt/dt-1.10.10/datatables.min.js"></script>
        <script type="text/javascript">
		$(document).ready(function(){
               
            	});
            	function sendMessage(){ // Send message to selected device
			var devId = $("tr.selected td:first" ).html();
			
			if( devId == undefined ) {
				alert("Don't select device to receice");
				return;
			}

	            	var d = document.getElementById("dataid");
	            	d.value = devId ;//$("tr.selected td:first" ).html();//e.options[e.selectedIndex].value;
	
	                var data = $('form#alluser').serialize();
	                $('form#alluser').unbind('submit');                
	                $.ajax({
	                    url: "send_message.php",// Really file to send message by GCM
	                    type: 'GET',
	                    data: data,
	                    beforeSend: function() {
	                        
	                    },
	                    success: function(data, textStatus, xhr) {
	                          $('.txt_message').val(""); // Init text area after sent message.
	                    },
	                    error: function(xhr, textStatus, errorThrown) {
	                        
	                    }
	                });
	                return false;
            }		
        </script>
        <style type="text/css">
            .container{
                width: auto;
                margin: 0 auto;
                padding: 0;
            }
            h1{
                font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
                font-size: 24px;
                color: #777;
            }
            div.clear{
                clear: both;
            }
            ul.devices{
                margin: 0;
                padding: 0;
            }
            ul.devices li{
                float: left;
                list-style: none;
                border: 1px solid #dedede;
                padding: 10px;
                margin: 0 15px 25px 0;
                border-radius: 3px;
                -webkit-box-shadow: 0 1px 5px rgba(0, 0, 0, 0.35);
                -moz-box-shadow: 0 1px 5px rgba(0, 0, 0, 0.35);
                box-shadow: 0 1px 5px rgba(0, 0, 0, 0.35);
                font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
                color: #555;
            }
            ul.devices li label, ul.devices li span{
                font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
                font-size: 12px;
                font-style: normal;
                font-variant: normal;
                font-weight: bold;
                color: #393939;
                display: block;
                float: left;
            }
            ul.devices li label{
                height: 25px;
                width: 50px;                
            }
            ul.devices li textarea{
                float: left;
                resize: none;
            }
            ul.devices li .send_btn{
                background: -webkit-gradient(linear, 0% 0%, 0% 100%, from(#0096FF), to(#005DFF));
                background: -webkit-linear-gradient(0% 0%, 0% 100%, from(#0096FF), to(#005DFF));
                background: -moz-linear-gradient(center top, #0096FF, #005DFF);
                background: linear-gradient(#0096FF, #005DFF);
                text-shadow: 0 1px 0 rgba(0, 0, 0, 0.3);
                border-radius: 3px;
                color: #fff;
            }
            td { cursor: pointer;}
		.selected {
			background-color: brown;
			color: #FFF;
		}
        </style>
    </head>
    <body>
        <?php
        include_once 'db_functions.php';
        $db = new DB_Functions();
        $users = $db->getAllUsers(); // Get all devices from database.
		if ($users != false)
            $no_of_users = mysql_num_rows($users);
        else
            $no_of_users = 0;
        ?>
        <div class="container">
            <img src="logo.png" />
            <hr/>
            <ul class="devices">
			<li>
			<form id="alluser" name="" method="post" onsubmit="return sendMessage();">
				<div class="clear"></div>
				<table id="table" style="table-layout: fixed;width:100%">
			            	<?php
					if ($no_of_users > 0) {
					?>
						<?php
						while ($row = mysql_fetch_array($users)) {
						?>
				    		<tr>
					        <td style="border: 1px #DDD solid;overflow:hidden;width:400px"><?php echo $row["gcm_regid"] ?></td>
					        <td width="5%" ><input type="button" id="<?php echo $row["gcm_regid"] ?>" value="Delete" onclick="fnselect(this)" /></td>
						</tr>
						<?php }
					} else { ?> 
						
					<?php } ?>
				</table>
				<div class="clear"></div>
				<div class="send_container">                                
					<textarea rows="3" name="data2" id="txaMsg" cols="114" class="txt_message" placeholder="Type message here"></textarea>
					<input type="hidden" name="data1" id="dataid" value=""/>
					<input type="submit" class="send_btn" value="Send" onclick=""/>
				</div>
			</form>
		</li>
            </ul>

        </div>
        <script>
	    	function highlight(e) {
			if (selected[0]) selected[0].className = '';
			e.target.parentNode.className = 'selected';
		}
	
		var table = document.getElementById('table'),
			selected = table.getElementsByClassName('selected');
		table.onclick = highlight;
	
		function fnselect(data){
			var devId = $("tr.selected td:first" ).html();
			
			if( devId == undefined ) {
				alert("Don't select device to delete");
				return;
			}
			//alert( devId );
			if( confirm("Are you sure to delete?" ) ){
				$.ajax({
		                    url: "delete_user.php",// Really file to send message by GCM
		                    type: 'POST',
		                    data: {regid:devId },
		                    success: function(result) {
		                        var d = document.getElementById("dataid");
			            	d.value = devId ;
		                        var txt = document.getElementById("txaMsg");
	            			txt.value = "delete_device";
	            			
	            			var data = $('form#alluser').serialize();
			                $.ajax({
			                    url: "send_message.php",// Really file to send message by GCM
			                    type: 'GET',
			                    data: data,
			                    beforeSend: function() {
			                        
			                    },
			                    success: function(data, textStatus, xhr) {
			                          $('.txt_message').val(""); // Init text area after sent message.
			                          window.location.href = "/gcm/gcm_update";
			                    },
			                    error: function(xhr, textStatus, errorThrown) {
			                        
			                    }
			                });
		                    },
		                    error: function() {
		                        alert("Fail");
		                    }
		                });
		          }
		}	
	</script>
    </body>
</html>