<!DOCTYPE html>

<html>
    <head>
        <title>BetterX Messages</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/r/bs-3.3.5/jq-2.1.4,dt-1.10.8/datatables.min.css"/>
		<script type="text/javascript" src="https://cdn.datatables.net/r/bs-3.3.5/jqc-1.11.3,dt-1.10.8/datatables.min.js"></script>
		<script type="text/javascript" charset="utf-8">
			$(document).ready(function() {
				$('#msgs').DataTable();
			} );
		</script>
    </head>
    <body>
        <?php
        include_once 'db_functions.php';
        $db = new DB_Functions();
        //$users = $db->getAllUsers(); // Get all devices from database.
		$msgs = $db->getAllMessages();	// Get all message log.	
		if ($msgs != false)
            $no_of_msgs = mysql_num_rows($msgs);
        else
            $no_of_msgs = 0;
        ?>
        <div class="container">
			<img src="logo.png" />
            <h1>Messages: <?php echo $no_of_msgs; ?></h1>
            <hr/>
            
		
		<table id="msgs" class="display" cellspacing="0" width="100%">
		<thead>
			<tr>
					<th>No</th>
					<th>Sender</th>
					<th>Receiver</th>
					<th>ID</th>
					<th>Msg</th>
					<th>Date</th>
			</tr>
		</thead>
		<tbody>
				<?php
					$i=0;
					while ($line = mysql_fetch_array($msgs)) {
						$i++;
				?>
				<tr id='idxtr_<?php echo $i; ?>' >
					<input type=hidden name='id<?php echo $i?>' value='<?php echo $line["id"];?>'>
					<td><?php echo $i; ?></td>
					<td><p align='center'><textarea name="sender_id" rows="1" cols="15"><?php echo $line["sender_id"];?></textarea></td>
					<td><p align='center'><textarea name=_ip1 rows="1" cols="15"><?php echo $line["receiver_id"];?></textarea></td>
					<td><p align='center'><textarea name=_ip1 rows="1" cols="15"><?php echo $line["msg_id"];?></textarea></td>						
					<td><p align='center'><textarea name="sender_id" rows="1" cols="60"><?php echo $line["msg_content"];?></textarea></td>
					<td><p align='center'><textarea name=_ip1 rows="1" cols="30"><?php echo $line["msg_tm"];?></textarea></td>
				</tr>
				<?php
				}
				?>
		</tbody>
		</table>
        </div>
		
		<script type="text/javascript">
		// For demo to fit into DataTables site builder...
		$('#msgs')
			.removeClass( 'display' )
			.addClass('table table-striped table-bordered');
		</script>
        
    </body>
</html>