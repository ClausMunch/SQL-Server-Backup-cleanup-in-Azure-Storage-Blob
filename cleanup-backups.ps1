Import-Module SQLPS -DisableNameChecking
Import-Module Az

function Get-TimeStamp {
    return "[{0:MM/dd/yy} {0:HH:mm:ss}]" -f (Get-Date)
}


## Config

# Backups to keep
$backupsToKeep = 2

# PSRobot
$PSRobotUsername='SQL user'
$PSRobotPassword='SQL user password'

# Instances
$SQLServers = @(
 'array of SQL Servers'
)

# Azure
$container="container name"
$StorageAccountName="Azure Storage Account Name"
$StorageAccountKey="Azure Storage Account Key"

## SCRIPT ##


#Script to delete backup files
$SQL_BackupHistory = @"
/*
D = Database 
I = Differential database 
L = Log 
F = File or filegroup 
G = Differential file 
P = Partial 
Q = Differential partial
*/

DECLARE @NumberOfBackupCopies INT = $($backupsToKeep);

WITH FullBackupSet
AS
(
SELECT
	bs.checkpoint_lsn,
	bs.database_backup_lsn,
	bs.database_name,
	RIGHT(bmf.physical_device_name, LEN(bmf.physical_device_name)-(CHARINDEX(@@SERVERNAME, bmf.physical_device_name)-1)) AS 'blobfilename',
	ROW_NUMBER() OVER (PARTITION BY database_name ORDER BY database_backup_lsn DESC) As RowNum
FROM 
	msdb.dbo.backupset bs
	JOIN msdb.dbo.backupmediafamily bmf  ON bs.media_set_id =  bmf.media_set_id
WHERE 
	type = 'D'
)

SELECT 
	checkpoint_lsn,
	database_backup_lsn,
	blobfilename
FROM 
	FullBackupSet
WHERE 
	RowNum <= @NumberOfBackupCopies
-- 
UNION
SELECT	
	bs.checkpoint_lsn,
	bs.database_backup_lsn,
	RIGHT(bmf.physical_device_name, LEN(bmf.physical_device_name)-(CHARINDEX(@@SERVERNAME, bmf.physical_device_name)-1)) AS 'blobfilename'
FROM
	msdb.dbo.backupset bs
	JOIN msdb.dbo.backupmediafamily bmf  ON bs.media_set_id =  bmf.media_set_id
WHERE
	bs.database_backup_lsn IN (SELECT checkpoint_lsn FROM FullBackupSet WHERE RowNum <= 2)
	AND type IN ('I')
UNION
SELECT	
	bs.checkpoint_lsn,
	bs.database_backup_lsn,
	RIGHT(bmf.physical_device_name, LEN(bmf.physical_device_name)-(CHARINDEX(@@SERVERNAME, bmf.physical_device_name)-1)) AS 'blobfilename'
FROM
	msdb.dbo.backupset bs
	JOIN msdb.dbo.backupmediafamily bmf  ON bs.media_set_id =  bmf.media_set_id
WHERE
	bs.database_backup_lsn IN (SELECT checkpoint_lsn FROM FullBackupSet WHERE RowNum <= 1)
--	AND bs.checkpoint_lsn IN ( SELECT MAX(checkpoint_lsn) FROM msdb.dbo.backupset bs WHERE type = 'I' OR type = 'D' GROUP BY database_name )
	AND type IN ('L')
"@
$SQL_ServerName = "SELECT @@SERVERNAME AS 'SQLInstance'"


# Retrieve backups list from Azure
$context = New-AzStorageContext -StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey
#$filelist = Get-AzStorageBlob -Container $container -Context $context -Prefix $sqlName

foreach($SQLServer in $SQLServers)
{
    $sqlName = (Invoke-Sqlcmd -Query "SELECT @@SERVERNAME AS 'ServerName'" -ServerInstance $SQLServer -Username $PSRobotUsername -Password $PSRobotPassword).ServerName.ToString().ToUpper()
    Write-Host "$(Get-TimeStamp) Running backup cleanup for $SQLServer ($sqlName)"
    $filelist = Get-AzStorageBlob -Container $container -Context $context -Prefix "$sqlName" ## /PlandayMaster/
    $connection = New-Object System.Data.SqlClient.SqlConnection
    $connection.ConnectionString = "server="+$SQLServer+";database=msdb;User Id="+$PSRobotUsername+";Password="+$PSRobotPassword    
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand

    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter.SelectCommand = $SqlCmd

    $SqlCmd.CommandText = $SQL_ServerName
    $SqlCmd.Connection = $connection
    $SQLInstance = New-Object System.Data.DataSet
    $SqlAdapter.Fill($SQLInstance) | Out-Null
#    $SQLInstance = $SqlCmd.ExecuteNonQuery();

    $SqlCmd.CommandText = $SQL_BackupHistory
    $SqlCmd.Connection = $connection
    $DataSet = New-Object System.Data.DataSet
    $SqlAdapter.Fill($DataSet) | Out-Null
    $connection.Close()
    
    ## $removeFiles = $filelist.name | Where-Object {(-not $DataSet.Tables[0].Select("blobfilename='"+$_+"'") )}
    $removeList = @()
    $keepList = @()

    ## $filelist -Azure Files
    ## $DataSet  -Valid Backups
    foreach ($backupfile in $filelist)
    {
        if($backupfile.Name -in $DataSet.Tables[0].blobfilename)
        {
            Write-Host -ForegroundColor Green "$(Get-TimeStamp) Keeping $($backupfile.Name)"
            $keepList = $keepList + $backupfile.Name
        }
        else
        {
            Write-Host -ForegroundColor Red "$(Get-TimeStamp) Removing $($backupfile.Name)"
            $removeList = $removeList + $backupfile.Name
        }
    }

    if($removeList.Count -gt 0)
    {
        foreach ($file in $removeList)
        {
            if (($null -ne $file) -and ($file.StartsWith(($SQLInstance.Tables[0] | ForEach-Object {$_.SQLInstance }).ToUpper()+"/")))
            {
                if($file.ICloudBlob.Properties.LeaseStatus -eq "Locked")
                {
                    Write-Host "$(Get-TimeStamp) A lease was locked on $file"
                }
                else
                {
                    Write-Host "$(Get-TimeStamp) Removing file $file"
                    Remove-AzStorageBlob -Blob $file -Container $container -Context $context
                }
            }
        }
    }
}