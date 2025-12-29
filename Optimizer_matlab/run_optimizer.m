function run_optimizer()
	


	currentFile = mfilename('fullpath');
	currentDir = fileparts(currentFile);

	logDir = fullfile(currentDir, '..', 'logs');
	if ~exist(logDir, 'dir')
		mkdir(logDir);
	end
	try
        date_suffix = datestr(now, 'yyyymmdd');
    catch
        % Fallback in case now() isn't available for some reason
        date_suffix = datestr(datetime('now'), 'yyyymmdd');
    end
	
	logFile = fullfile(logDir, sprintf('weight_optimizer_%s.log', date_suffix));
	try
		% diary(file) turns on logging to that file
		diary(logFile);
		fprintf_log('Run optimizer logging to: %s\n', logFile);
	catch ME
		warning('Could not start diary log to %s: %s', logFile, ME.message);
	end
	% Ensure diary is turned off on function exit
	cleanupDiary = onCleanup(@() diary('off'));

	currentFile = mfilename('fullpath');
	currentDir = fileparts(currentFile);
	path_config = fullfile(currentDir, '..','config', 'paths.yaml');
	path = ReadYaml(path_config);
	addpath(genpath(path.yaml_matlab));

	savepath;
	data_preparation();
	batch_run_optimizer();

	path_config = fullfile(currentDir, '..','config', 'paths.yaml');

	path = ReadYaml(path_config);

	addpath(fullfile(currentDir, 'utils'));
	addpath(fullfile(currentDir, 'tools'));

	merge_portfolio_dataframe(path.tempp_dir);

	


