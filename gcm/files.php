<!DOCTYPE html>

<html>
    <head>
        <title>BetterX Files</title>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/r/bs-3.3.5/jq-2.1.4,dt-1.10.8/datatables.min.css"/>
		<script type="text/javascript" src="https://cdn.datatables.net/r/bs-3.3.5/jqc-1.11.3,dt-1.10.8/datatables.min.js"></script>
		<script type="text/javascript" charset="utf-8">
			$(document).ready(function() {
				$('#files').DataTable();
			} );
		</script>
    </head>
    <body>
        <?php
        include_once 'db_functions.php';
        $db = new DB_Functions();
        $files = $db->getAllFiles();
		if ($files != false)
            $no_of_files = mysql_num_rows($files);
        else
            $no_of_files = 0;
        ?>
        <div class="container">
			<img src="logo.png" />
            <h1>Files: <?php echo $no_of_files; ?></h1>
            <hr/>
            
		<table id="files" class="display" cellspacing="0" width="100%">
		<thead>
			<tr>
					<th>No</th>
					<th>Zip</th>
					<th>Name</th>
					<th>Status</th>
					<th>ZipFile</th>
					<th>ZipFileNo</th>
					<th>ZipFileTotal</th>
			</tr>
		</thead>
		<tbody>
				<?php
					$i=0;
					while ($line = mysql_fetch_array($files)) {
						$i++;
				?>
				<tr id='idxtr_<?php echo $i; ?>' >
					<td><?php echo $line["id"];?></td>
					<td><p align='center'><textarea name="FILEZIP" rows="1" cols="15"><?php echo $line["file_zip"];?></textarea></td>
					<td><p align='center'><textarea name="FILENAME" rows="1" cols="15"><?php echo $line["file_name"];?></textarea></td>
					<td><?php echo $line["status"];?></td>
					<td><?php echo $line["zip_file"];?></td>						
					<td><?php echo $line["zip_file_no"];?></td>
					<td><?php echo $line["zip_file_total"];?></td>
				</tr>
				<?php
				}
				?>
		</tbody>
		</table>
        </div>
		
		<script type="text/javascript">
		// For demo to fit into DataTables site builder...
		$('#files')
			.removeClass( 'display' )
			.addClass('table table-striped table-bordered');
		</script>
        
    </body>
</html>