
## Created by: Synthetic009@gmail.com
# For demo purposes only, showcasing multithreading in a single script using runspaces, threads, and load testing
# Default log location: C:\PD\Data\Log\

function Write-Log 
{
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true)]
        [ValidateNotNullOrEmpty()]
        [Alias("LogContent")]
        [string]$Message,

        # Replace
        [Alias('LogPath')]
        [string]$Path="C:\PD\Data\log\"+"PDMultiThreadExample.log",
        [ValidateSet("Error","Warn","Info")]
        [string]$Level="Info",
        [switch]$NoClobber
    )

    BEGIN 
    {
        $VerbosePreference = 'Continue'
    }
    PROCESS 
    { 
        If ((Test-Path $Path) -AND $NoClobber) 
        {
            Write-Error "Log file $Path already exists, and you specified NoClobber. Either delete the file or specify a different name." -ErrorAction Stop
        }
        ElseIf (!(Test-Path $Path)) 
        {
            Write-Verbose "Creating $Path."
            New-Item $Path -Force -ItemType File | Out-Null
        }

        $FormattedDate = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $outputMessage = ""

        Switch ($Level) 
        {
            'Error' 
            {
                $LevelText = 'ERROR:'
                $outputMessage =  "$FormattedDate $LevelText $Message"
                $outputMessage | Out-File -FilePath $Path -Append 
                Write-Error $outputMessage
            }
            'Warn' 
            {
                $LevelText = 'WARNING:'
                $outputMessage =  "$FormattedDate $LevelText $Message"
                Write-Warning $outputMessage
            }
            'Info' 
            {
                $LevelText = 'INFO:'
                $outputMessage =  "$FormattedDate $LevelText $Message"
                Write-Verbose $outputMessage
            }
        }
        "$FormattedDate $LevelText $Message" | Out-File -FilePath $Path -Append 
    }
    END { }
}
function New-Thread
{
    param(
        [parameter(mandatory=$true)]$runspacepool,
        [parameter(mandatory = $true)]$arrayElement,
        [parameter(mandatory = $true)]$action
    )
    #advised to disable this logging after debugging
    #we expect the runspacepool, arrayElement (the single item from the array you're working on), and the action
    write-log "============================"
    write-log "Start of New-Thread"
    write-log "PARAM arrayElement: $arrayElement"
    write-log "PARAM action: $action"

    #the scriptblock contains the items that will be passed to the thread, if it's not defined here, the thread wont get it
    $scriptblock = New-Object psobject | Add-Member -NotePropertyName arrayElement -NotePropertyValue $arrayElement -Force -PassThru `
    | Add-Member -NotePropertyName action -NotePropertyValue $action -Force -PassThru 
    try
    {
        $powershell = [powershell]::Create()
        $powershell.RunspacePool = $runspacepool | Out-Null 
        [void]$powershell.AddScript(
        {
            param(
                [parameter(mandatory = $true)]$scriptblock
            )
            try
            {
                $result = $false;
                Function Test-ThreadFunction
                {
                    Param (
                        [Parameter(Mandatory=$true)][string]$element,
                        [Parameter(Mandatory=$true)][string]$action,
                        [Parameter(Mandatory=$false)][string]$info="DefaultInfo",
                        [Parameter(Mandatory=$false)][int]$waitTime = 25,
                        [Parameter(Mandatory=$false)][string]$threadLogPath = "C:\PD\Data\Log\"
                    )
                    try 
                    {
                        #this test function will populate a text file with meaningless test data as an example
                        $result = $false;
                        $threadLogPath= $threadLogPath+"$element"+"-$action"+".log"
                        #create the file if it doesn't exist (it shouldn't)
                        if (!(test-path -Path $threadLogPath))
                        {
                            $resultNewItem = new-item -Path $threadLogPath -Force
                            $resultNewItem | out-file $threadLogPath -Append
                        }
                        #output some data to log file
                        $result = "THREAD RETURN DATA: [Date: [$(get-date)] ; Element Data: [$element] ; Info: [$info] ; Action: [$action] ; WaitTime: [$WaitTime] ; OutputPath: [$threadLogPath]]"
                        $result | out-file $threadLogPath -Append

                        #simulate work, output to our log file
                        [int]$counter = 0;
                        [int]$exitCount = 5;
                        while ($counter -lt $exitCount)
                        {
                            "TEST DATA [$counter]" | out-file $threadLogPath -Append
                            $counter++
                        }
                    }
                    catch 
                    {
                        $ErrorMessage = $_.Exception.Message
                        $result = "Encounted Error while executing [Test-ThreadFunction], error: $ErrorMessage"   
                    }
                    return $result
                }
                $result = Test-ThreadFunction -element $($scriptblock.arrayElement[0]) -action $($scriptblock.action) -info $($scriptblock.arrayElement[1])
                return $result
            }
            catch
            {
                $ErrorMessage = $_.Exception.Message
                Write-Error $ErrorMessage
            }
        }, $true).addparameter('scriptblock',$scriptblock) #Setting UseLocalScope to $True
        ### Fix for multithreading variable scope issue: 
        ### https://learn-powershell.net/2018/01/28/dealing-with-runspacepool-variable-scope-creep-in-powershell/

        $Handle = $powershell.BeginInvoke()
    
        $ThreadData = New-Object psobject | Add-Member -NotePropertyName Powershell -NotePropertyValue $powershell -Force -PassThru `
        | Add-Member -NotePropertyName Handle -NotePropertyValue $Handle -Force -PassThru `
        | Add-Member -NotePropertyName arrayElement -NotePropertyValue $($scriptblock.arrayElement) -Force -PassThru `
        | Add-Member -NotePropertyName action -NotePropertyValue $($scriptblock.action) -Force -PassThru `
        | Add-Member -NotePropertyName IsMarkedComplete -NotePropertyValue $false -Force -PassThru
        ### ERRORS ARE EXPOSD AFTER ENDINVOKE IS CALLED, OUTSIDE THIS FUNCTION
        ### NEED TO CREATE CLEANUP FUNCTION FOR THESE THREADS OUTSIDE THIS FUNCTION

        return ,$ThreadData
    }
    catch
    {
        $ErrorMessage = $_.Exception.Message
        Write-Log -Level Warn "Failed inside New-Thread, error: $ErrorMessage"
        return $false
    }
}
function New-RunspacePool
{
    Param(
        [Parameter(Mandatory=$false)][array]$ModulesToLoad = @(),
        [Parameter(Mandatory=$false)][int]$ThreadMin = 1,
        [Parameter(Mandatory=$true)][int]$ThreadMax
    )
    write-log "================================================="
    write-log "New-RunspacePool"
    write-log "Passed ModulesToLoad: $ModulesToLoad"
    write-log "Passed ThreadMin: $ThreadMin"
    write-log "Passed ThreadMax: $ThreadMax"

    #You can pass modules you'd like to load as an array into this function, it will get included into the sessionstate and become available to the thread
    $sessionstate = $null
    $runspacepool = $null
    try
    {
        $sessionstate = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
        if ($ModulesToLoad)
        {
            foreach ($module in $ModulesToLoad)
            {
                write-log "Import PSModule Name: ($module) into SessionState..."
                $sessionstate.ImportPSModule($module) 
            }
        }
        else
        {
            write-log "No modules to load inside doCreateRunspacePool."
        }
        write-log "Creating RunspacePool."
        $runspacepool = [runspacefactory]::CreateRunspacePool($ThreadMin,$ThreadMax,$sessionstate,$Host)
        $runspacepool.Open()

        return $runspacepool
    }
    catch
    {
        $ErrorMessage = $_.Exception.Message
        write-log -Level Error "Error while creating runspacePool, error: $ErrorMessage" 
        return $false
    }
}
function Read-CPUCapacity
{
    Param(
        [Parameter(Mandatory=$false)][ValidateSet("Windows","Linux")]$OperatingSystem = "Windows"
    )
    write-log "================================================="
    write-log "Read-CPUCapacity"
    $result = $false;
    if ($OperatingSystem -eq "Windows")
    {
        $result = [int]$env:NUMBER_OF_PROCESSORS
    }
    elseif ($OperatingSystem -eq "Linux")
    {
        try 
        {
            $lscpu = lscpu
            for ($i=0; $i -lt $lscpu.count; $i++)
            {
                #write-log "Item: $i : $($lscpu[$i])"
                if ($lscpu[$i] -like "CPU(s)*")
                {
                    #write-log "found CPU count..."
                    $split = $lscpu[$i].Split(":")
                    $result = $split[-1].Trim()
                    break;
                }
            }
        }
        catch 
        {
            $ErrorMessage = $_.Exception.Message
            write-log -Level Error "Error while retrieving Linux CPU Info, error: $ErrorMessage" 
            return $false
        }
    }
    return $result
}
function Resolve-ThreadManager
{
    Param(
        [Parameter(Mandatory=$true)][array]$ThreadManager,
        [parameter(mandatory=$true)]$runspacepool
    )
    write-log "================================================="
    write-log "Resolve-ThreadManager"
    
    if ($ThreadManager)
    {
        ### removes completed threads from threadmanager to pass back, completed items will be evaluated after this
        $newThreadManager = @();
        for ($i=0;$i -lt $ThreadManager.Count; $i++)
        {
            if (!($ThreadManager[$i].IsMarkedComplete))
            {
                $newThreadManager+=,$($ThreadManager[$i])
            }
        }
        $ThreadManager = $newThreadManager
    }

    if (!($ThreadManager))
    {
        #write-log "No active threads inside ThreadManager, returning."
        return ,$ThreadManager
    }
    else
    {
        try
        {
            # demo, see below commented out code
            #[array]$tempThreadManager = @();
            for ($i=0;$i -lt $ThreadManager.Count; $i++)
            {
                if ($ThreadManager[$i].IsMarkedComplete)
                {
                    write-log "Removed this item! it's Marked Complete dummy!"
                    continue;
                }
                elseif (($ThreadManager[$i].Handle.iscompleted -eq $true) -and ($ThreadManager[$i].powershell.Runspace.RunspaceAvailability -ne "None"))
                {
                    try
                    {
                        $ThreadManager[$i] | Add-Member -NotePropertyName Errors -NotePropertyValue $($ThreadManager[$i].powershell.streams.error) -Force 
                        $ThreadManager[$i] | Add-Member -NotePropertyName Result -NotePropertyValue $($ThreadManager[$i].powershell.EndInvoke($ThreadManager[$i].Handle)) -Force 
                        if ($ThreadManager[$i].Errors)
                        {
                            write-log "Thread finished with one or more errors..."
                            #for debugging, careful referencing items by property name, you can change this
                            write-log "Task: [$($ThreadManager[$i].action)] completed with one or more errors. Errors: [$($ThreadManager[$i].Errors)]" -Level Warn
                            #stop evaluating, move to next object
                            continue;
                        }
                        else 
                        {

                            ###Evaluate result to determine if new actions are needed
                            #this is optional
                            switch ("$($ThreadManager[$i].action)")
                            {
                                default
                                {
                                    #write-log "default switch option for [Resolve-Threadmanager]"
                                    write-log "Result: Command: [$($ThreadManager[$i].action)] ; Element: [$($ThreadManager[$i].arrayElement)] ; Result: $($ThreadManager[$i].Result)"
                                }
                            }
                        }
                        
                    }
                    catch
                    {
                        $ErrorMessage = $_.Exception.Message
                        write-log -Level Warn "Unable to complete thread evaluation for action: [$($ThreadManager[$i].action)] , error: $ErrorMessage" 
                        continue;
                    }
                    finally
                    {
                        #mark completed and dispose of runspace
                        $ThreadManager[$i].IsMarkedComplete = $true
                        $ThreadManager[$i].powershell.Runspace.Dispose()
                        $ThreadManager[$i].powershell.Dispose()
                    }
                }
                else
                {
                    # for debugging
                    #write-log "Thread iscompleted: $($ThreadManager[$i].Handle.iscompleted)" 
                    #write-log "Thread powershell.Runspace.RunspaceAvailability: $($ThreadManager[$i].powershell.Runspace.RunspaceAvailability) "
                }
            }  
        }
        catch
        {
            $ErrorMessage = $_.Exception.Message
            write-log -Level Warn "Unable to access Thread inside ThreadManager, error: $ErrorMessage" 
            return $false
        }
    }
    return ,$ThreadManager
}
function Invoke-MultiExecute
{
    Param(
        [Parameter(Mandatory=$true)][array]$array,
        [Parameter(Mandatory=$true)][string]$action
    )
    try 
    {
        write-log "================================================="
        write-log "Invoke-MultiExecute"
        write-log "Array Count: $($array.Count)" 
        write-log "Action: $($action)" 
        
        #default result as false, true if we succeed
        $result = $false;
        #define how many times we'll loop over thread evaluation to wait for it to finish processing, time = count * 1 second
        #if we break out of the evaluate loop and there are more items to be processed, we'll kick it out by adding a new thread to be processed
        [int]$stopCount = 30;
        [int]$sleepTime = 1;

        #change this to your OS!
        $CPUCapacity = Read-CPUCapacity -OperatingSystem "Windows"
        ### OVERWRITE MAX THREADS HERE OR USE ABOVE FUNCTION
        #$CPUCapacity = 20;
        if (!$CPUCapacity)
        {
            Write-Log -Level Error "Failed to retrieve CPU Capacity, exiting."
            return $result;
        }

        $RunSpacePool = New-RunspacePool -ThreadMax $CPUCapacity
        if (!$RunSpacePool)
        {
            Write-Log -Level Error "Failed to create RunspacePool, exiting."
            return $result;
        }

        #create Thread Manager, will be used to interact with runspace pool
        $ThreadManager = @();

        #attempt to create a thread for each element passed to this function
        for ($i=0; $i -lt $array.count; $i++)
        {
            #incrementions on each loop of evaluation, paired with stopCount to determining state
            [int]$EvalCounter = 0;
            #we loop through this after we hit max capacity, pay attention to the exit condition here
            while ($ThreadManager.Count -eq $CPUCapacity)
            {
                if ($EvalCounter -gt $stopCount)
                {
                    write-log -Level Warn "Excess time spent evaluating threads, exiting at [$stopCount]"
                    break;
                }
                $EvalCounter++
                start-sleep -Seconds $sleepTime  | Out-Null

                $EvaluateThreadManager = Resolve-ThreadManager -ThreadManager $ThreadManager -runspacepool $RunSpacePool
                if ($EvaluateThreadManager)
                {
                    $ThreadManager = $EvaluateThreadManager
                }
                else 
                {
                    #null or empty result, break out and continue on
                    break;
                }
            }
            
            #Spin off new thread, return will be thread object, store in threadmanager
            $NewThreadResult = New-Thread -runspacepool $RunspacePool -arrayElement $array[$i] -action $action
            if ($NewThreadResult)
            {
                #add the result into our thread manager
                $ThreadManager+= $NewThreadResult 
                
                #evaluate results 
                $EvaluateThreadManager = $null
                $EvaluateThreadManager = Resolve-ThreadManager -ThreadManager $ThreadManager -runspacepool $RunSpacePool
                if ($EvaluateThreadManager)
                {
                    #on successful evaluate, reset the thread manager to the return result
                    $ThreadManager = $EvaluateThreadManager
                }
                else
                {
                    break;
                }
            }
            else
            {
                $ErrorMessage = $_.Exception.Message
                write-log -Level warn "Failed to create new thread, investigate. Error: $ErrorMessage"
            }
        }

        #all threads should be created and either processed or in flight at this point
        #we now need to make sure they're all processed before moving on

        #reset counter
        [int]$EvalCounter = 0;
        
        #thread is in flight, attempt to resolve
        while ($ThreadManager)
        {
            if ($EvalCounter -gt $stopCount)
            {
                write-log -Level Warn "Excess time spent evaluating threads, exiting at [$stopCount]"
                break;
            }
            $EvalCounter++
            start-sleep -Seconds $sleepTime
            $EvaluateThreadManager = Resolve-ThreadManager -ThreadManager $ThreadManager -runspacepool $RunSpacePool
            if ($EvaluateThreadManager)
            {
                $ThreadManager = $EvaluateThreadManager
            }
            else 
            {
                #write-log "ThreadManager finished resolving items, exit loop"
                break;   
            }
        }
        $result = $true;
    }
    catch 
    {
        $ErrorMessage = $_.Exception.Message
        write-log -level Warn "Encounted Error while executing [Invoke-MultiExecute], error: $ErrorMessage"
        $result = $false
    }
    finally
    {
        #add evaluation on return threads later
    }
    return $result
}

# START
try 
{
    # define some collection of items to work on, typically by calling some function, but can just define it like below:
    $ExampleArray = @();
    [int]$itemsToGenerate = 500;
    for ($i=0; $i -lt  $itemsToGenerate; $i++)
    {
        $ExampleArray+=,@("Example$i","Info$i")
    }

    # define some action to take on these items
    [string]$ExampleAction = "ExampleAction"
    
    if ($ExampleArray -and $ExampleAction)
    {

        #Write-host "༼ つ ◕_◕ ༽つ LET'S DO THiS! ༼ つ ◕_◕ ༽つ" -ForegroundColor Green
        write-host "Invoking Multithreaded Execution Function"
        $StopWatch = [system.diagnostics.stopwatch]::StartNew()
        $resultMultiExecute = Invoke-MultiExecute -array $ExampleArray -action $ExampleAction
        $StopWatch.Stop()
        $StopWatch.Elapsed.TotalMilliseconds
        #Write-host "༼༼ つ ◕_◕ ༽つ °º¤ø,¸¸,ø¤º°`°º¤ NICE! GOOD JOB! °º¤ø,¸¸,ø¤º°`°º¤ ༼ つ ◕_◕ ༽つ" -ForegroundColor Green
        return $resultMultiExecute
    }
}
catch 
{
    $ErrorMessage = $_.Exception.Message
    write-log -level Warn "Encounted Error while executing, error: $ErrorMessage"
    #Write-host "༼ つ ◕_◕ ༽つ  SOMETHING IS SCUFFED, FIX IT!༼ ༼ つ ◕_◕ ༽つ" -ForegroundColor Red
    return $false
}