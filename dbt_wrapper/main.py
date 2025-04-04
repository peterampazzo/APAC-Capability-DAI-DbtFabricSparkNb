from typing import Optional
import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.theme import Theme
from dbt_wrapper.wrapper import Commands
from rich import print
from dbt_wrapper.log_levels import LogLevel
from dbt_wrapper.hashcheck_levels import HashCheckLevel
from dbt_wrapper.stage_executor import stage_executor


app = typer.Typer(no_args_is_help=True)

custom_theme = Theme({"info": "dim cyan", "warning": "dark_orange", "danger": "bold red", "error": "bold red", "debug": "khaki1"})

console = Console(theme=custom_theme)

wrapper_commands = Commands(console=console)

_log_level: LogLevel = None

if (_log_level is None):
    _log_level = LogLevel.WARNING

#JM issues61 adding _hashcheck_level
_hashcheck_level: HashCheckLevel = None
if (_hashcheck_level is None):
    _hashcheck_level = HashCheckLevel.BYPASS

def docs_options():
    return ["generate", "serve"]

def log_levels():
    return ["DEBUG", "INFO", "WARNING", "ERROR"]

#JM issues61 adding _hashcheck_level
def hashcheck_levels():
    return ["BYPASS", "WARNING", "ERROR"]

@app.command()
def docs():
    """
    This command will generate the documentation for the dbt project.
    """
    print(f"Goodbye")


@app.command()
def buildcomparemetadata(  
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    source: Annotated[
        str,
        typer.Argument(
            help="Source environment name from profile.yml"
        ),
    ],
    target: Annotated[
        str,
        typer.Argument(
            help="Target environment name from profile.yml"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None
    ):
    """
    This command will execute the 'build metadata' notebooks in the two specified environments within Fabric. This notebook will create a `comparemetadata` table in their respective environment that will store the Lakehouse schema information.
    """
    log_level = "WARNING"

    _log_level: LogLevel = LogLevel.from_string(log_level)    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir, source_env=source, target_env=target)

    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    se.perform_stage(option=True, action_callables=[wrapper_commands.RunBuildMetadataNotebook_Source], stage_name=f"Run Build Metadata Notebook (Source: {source})")
    se.perform_stage(option=True, action_callables=[wrapper_commands.RunBuildMetadataNotebook_Target], stage_name=f"Run Build Metadata Notebook (Target: {target})")


    print(f"Goodbye")


@app.command()
def compare(  
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    source: Annotated[
        str,
        typer.Argument(
            help="Source environment name from profile.yml"
        ),
    ],
    target: Annotated[
        str,
        typer.Argument(
            help="Target environment name from profile.yml"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None):
    """
    This command will compare two environments Lakehouse's. Generated notebooks will be uploaded to the 'target' 
    environment configured in the 'profile.yml' file which will contain the sql commands that can be used to 
    create and alter tables, from one environment to the next.
    """
    log_level = "WARNING"

    _log_level: LogLevel = LogLevel.from_string(log_level)    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir, source_env=source, target_env=target)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)

    se.perform_stage(option=True, action_callables=[wrapper_commands.GenerateCompareNotebook], stage_name="Generate Compare Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.ConvertNotebooksToFabricFormat], stage_name="Convert to Fabric Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.UploadCompareNotebookViaApi], stage_name="Upload Compare Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.RunCompareNotebook], stage_name="Run Compare Notebook")


    # #download the metadata
    se.perform_stage(option=True, action_callables=[wrapper_commands.DownloadMetadata], stage_name="Download Metadata")

    se.perform_stage(option=True, action_callables=[wrapper_commands.GenerateMissingObjectsNotebook], stage_name="Generate Missing Objects Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.ConvertNotebooksToFabricFormat], stage_name="Convert to Fabric Notebook")
    se.perform_stage(option=True, action_callables=[wrapper_commands.UploadMissingObjectsNotebookViaApi], stage_name="Upload Missing Objects Notebook to Target Workspace")




    print(f"Goodbye")

@app.command()
def run_all(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None,
    clean_target_dir: Annotated[
        bool,
        typer.Option(
            help="The option to clear out the target folder before dbt project build."
        ),
    ] = True,
    generate_pre_dbt_scripts: Annotated[
        bool,
        typer.Option(
            help="The option to generate the pre dbt scripts before dbt project build."
        ),
    ] = True,
    generate_post_dbt_scripts: Annotated[
        bool,
        typer.Option(
            help="The option to generate the post dbt scripts before dbt project build."
        ),
    ] = True,
    auto_execute_metadata_extract: Annotated[
        bool,
        typer.Option(
            help="The option to automatically refresh metadata before dbt project build."
        ),
    ] = True,
    download_metadata: Annotated[
        bool,
        typer.Option(
            help="The option to automatically download metadata before dbt project build."
        ),
    ] = True,
    build_dbt_project: Annotated[
        bool,
        typer.Option(
            help="The option to suppress build the dbt project."
        ),
    ] = True,
    pre_install: Annotated[
        bool,
        typer.Option(
            help="The option to run the dbt adapter using source code and not the installed package."
        ),
    ] = False,
    upload_notebooks_via_api: Annotated[
        bool,
        typer.Option(
            help="The option to upload your notebooks directly via the powerbi api."
        ),
    ] = True,
    auto_run_master_notebook: Annotated[
        bool,
        typer.Option(
            help="The option to automatically execute your transformation pipeline by executing the master orchestration notebook after the build and publish stages."
        ),
    ] = True,
    log_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the log level. This controls the verbosity of the output. Allowed values are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default is `WARNING`.",
        ),
    ] = "WARNING",
    #JM issues61 adding _hashcheck_level
    hashcheck_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the hash check level. This controls the verbosity of the output. Allowed values are `BYPASS`, `WARNING`, `ERROR`. Default is `BYPASS`.",
        ),
    ] = "BYPASS",
    notebook_timeout: Annotated[
        int,
        typer.Option(
            help="Use this option to change the default notebook execution timeout setting.",
        ),
    ] = 1800
    ,
    select: Annotated[
        str,
        typer.Option(
            help="Use this option to provide a dbt resource selection syntax.Default is ``",
        ),
    ] = ""
    ,
    exclude: Annotated[
        str,
        typer.Option(
            help="Use this option to provide a dbt resource exclude syntax.Default is ``",
        ),
    ] = ""
    ,
    lakehouse_config: Annotated[
        Optional[str],
        typer.Option(
            help="Use this option to set the default lakehouse in code or metadata. Allowed values are `CODE` or `METADATA`. Default is `METADATA`.",
        ),
    ] = "METADATA"
):
    """
    This command will run all elements of the project. For more granular control you can use the options provided to suppress certain stages or use a different command.
    """    
    
    _log_level: LogLevel = LogLevel.from_string(log_level)    
    #JM issues61 adding _hashcheck_level
    _hashcheck_level: HashCheckLevel = HashCheckLevel.from_string(hashcheck_level)

    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    se.perform_stage(option=clean_target_dir, action_callables=[wrapper_commands.CleanProjectTargetDirectory], stage_name="Clean Target")

    action_callables = [
        lambda **kwargs: wrapper_commands.GeneratePreDbtScripts(PreInstall=pre_install, notebook_timeout=notebook_timeout, lakehouse_config=lakehouse_config, **kwargs),
        lambda **kwargs: wrapper_commands.ConvertNotebooksToFabricFormat(lakehouse_config=lakehouse_config, **kwargs),
    ]
    se.perform_stage(option=generate_pre_dbt_scripts, action_callables=action_callables, stage_name="Generate Pre-DBT Scripts")

    se.perform_stage(option=auto_execute_metadata_extract, action_callables=[wrapper_commands.RunMetadataExtract], stage_name="Auto Execute Metadata Extract")

    se.perform_stage(option=download_metadata, action_callables=[wrapper_commands.DownloadMetadata], stage_name="Download Metadata")

    if (build_dbt_project):
        wrapper_commands.BuildDbtProject(PreInstall=pre_install, select=select, exclude=exclude)

#JM issues61 adding _hashcheck_level
    action_callables = [
        lambda **kwargs: wrapper_commands.GeneratePostDbtScripts(PreInstall=pre_install, notebook_timeout=notebook_timeout, notebook_hashcheck=_hashcheck_level, lakehouse_config=lakehouse_config, **kwargs),
        lambda **kwargs: wrapper_commands.ConvertNotebooksToFabricFormat(lakehouse_config=lakehouse_config, **kwargs)
    ]
    se.perform_stage(option=generate_post_dbt_scripts, action_callables=action_callables, stage_name="Generate Post-DBT Scripts")    

    se.perform_stage(option=upload_notebooks_via_api, action_callables=[wrapper_commands.AutoUploadNotebooksViaApi], stage_name="Upload Notebooks via API")

    se.perform_stage(option=auto_run_master_notebook, action_callables=[wrapper_commands.RunMasterNotebook], stage_name="Run Master Notebook")
    se.perform_stage(option=auto_run_master_notebook, action_callables=[wrapper_commands.GetExecutionResults], stage_name="Get Execution Results")


@app.command()
def download_metadata(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None,    
    log_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the log level. This controls the verbosity of the output. Allowed values are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default is `WARNING`.",
        ),
    ] = "WARNING"
):
    """
    This command will run just the metadata download.
    """    
    
    _log_level: LogLevel = LogLevel.from_string(log_level)    
    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    
    se.perform_stage(option=download_metadata, action_callables=[wrapper_commands.DownloadMetadata], stage_name="Download Metadata")


@app.command()
def get_execution_results(
    dbt_project_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_project directory. If left blank it will use the current directory"
        ),
    ],
    dbt_profiles_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the dbt_profile directory. If left blank it will use the users home directory followed by .dbt."
        ),
    ] = None,    
    log_level: Annotated[
        Optional[str],
        typer.Option(
            help="The option to set the log level. This controls the verbosity of the output. Allowed values are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default is `WARNING`.",
        ),
    ] = "WARNING"
):
    """
    This command will run just the extract of the last execution results.
    """    
    
    _log_level: LogLevel = LogLevel.from_string(log_level)    
    
    wrapper_commands.GetDbtConfigs(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir)
    se: stage_executor = stage_executor(log_level=_log_level, console=console)
    
    se.perform_stage(option=True, action_callables=[wrapper_commands.GetExecutionResults], stage_name="Get Execution Results")


if __name__ == "__main__":
    app()
