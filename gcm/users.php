<!DOCTYPE html>

<html>
    <head>
        <title>BetterX Users</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/r/bs-3.3.5/jq-2.1.4,dt-1.10.8/datatables.min.css"/>
		<script type="text/javascript" src="https://cdn.datatables.net/r/bs-3.3.5/jqc-1.11.3,dt-1.10.8/datatables.min.js"></script>
		<script type="text/javascript" charset="utf-8">
			$(document).ready(function() {
				$('#users').DataTable();
			} );
		</script>
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
            <h1>Users: <?php echo $no_of_users; ?></h1>
            <hr/>
            
		<table id="users" class="display" cellspacing="0" width="100%">
		<thead>
			<tr>
					<th>No</th>
					<th>GCMID</th>
					<th>IMEI</th>
					<th>MEID</th>
					<th>SIMSERIAL</th>
					<th>ANDROIDID</th>
					<th>DATE ADDED</th>
					<th>DATE REMOVED</th>
			</tr>
		</thead>
		<tbody>
				<?php
					$i=0;
					while ($line = mysql_fetch_array($users)) {
						$i++;
				?>
				<tr id='idxtr_<?php echo $i; ?>' >
					<td><?php echo $i; ?></td>
					<td><p align='center'><textarea name="GCMID" rows="1" cols="15"><?php echo $line["gcm_regid"];?></textarea></td>
					<td><?php echo $line["imei"];?></td>
					<td><?php echo $line["meid"];?></td>						
					<td><?php echo $line["simserial"];?></td>
					<td><?php echo $line["androidid"];?></td>
					<td><?php echo $line["created"];?></td>
					<td><?php echo $line["removed"];?></td>
				</tr>
				<?php
				}
				?>
		</tbody>
		</table>
        </div>
		
		<script type="text/javascript">
		// For demo to fit into DataTables site builder...
		$('#users')
			.removeClass( 'display' )
			.addClass('table table-striped table-bordered');
		</script>
        
    </body>
</html>