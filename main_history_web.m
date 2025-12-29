currentFile = mfilename('fullpath');
currentDir = fileparts(currentFile);


addpath(fullfile(currentDir, 'Optimizer_matlab'));	
% run_optimizer();
[portfolio_info, ~, ~] = ConfigReader_sql();
user_names = portfolio_info.user_name;

try
    pyScript = fullfile(currentDir, 'import_weight_to_mysql_custom.py');

    path_config = fullfile(currentDir, 'config', 'paths.yaml');
    try
        paths_cfg = ReadYaml(path_config);
        if isfield(paths_cfg, 'python_exe') && ~isempty(paths_cfg.python_exe)
            pythonExe = paths_cfg.python_exe;
        else
            % fallback to system python on PATH
            pythonExe = 'python';
        end
    catch
        % if ReadYaml is not available or loading fails, fallback to system python
        pythonExe = 'python';
    end
    % existence checks for easier diagnosis
    if ~isfile(pythonExe)
        fprintf_log('Python executable not found: %s', pythonExe);
    elseif ~isfile(pyScript)
        fprintf_log('Python script not found: %s', pyScript);
    else

    if iscell(user_names)
        uname = user_names{1};
    elseif isstring(user_names)
        % take the first element if it's a string array
        uname = char(user_names(1));
    elseif ischar(user_names)
        uname = user_names;
    else
        % fallback: convert to char (numeric -> string)
        try
            uname = char(user_names);
        catch
            uname = num2str(user_names);
        end
    end

    sid = '';
 
    try
        if exist('portfolio_info', 'var') && ~isempty(portfolio_info)
            if ismember('session_id', portfolio_info.Properties.VariableNames)
                sid = char(string(portfolio_info.session_id(1)));
            end
           
        end
    catch
       
        sid = '';
 
    end
   
      
    % build python command; include session_id and id when present
    if ~isempty(sid) 
        cmd = sprintf('"%s" "%s" "%s" "%s" "%s"', pythonExe, pyScript, uname, sid);
 
    end
        [status, cmdout] = system(cmd);
        fprintf_log('Python return status: %d', status);
        if ~isempty(cmdout)
            fprintf_log('Python output:\n%s', cmdout);
        end
        if status ~= 0
            fprintf_log('Running python script failed (status=%d).', status);
        end
    end
catch ME
    % Use identifier-aware warning format to satisfy MATLAB diagnostics
    if isprop(ME, 'identifier') && ~isempty(ME.identifier)
        id = ME.identifier;
    else
        id = 'run_optimizer:pythonImportFail';
    end
    fprintf_log(id, 'Failed to launch Python importer: %s', ME.message);
end
