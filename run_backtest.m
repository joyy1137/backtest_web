clear; clc; close all;


% 配置参数
script_dir = fileparts(mfilename('fullpath'));
% 分离输入和输出路径
currentFile = mfilename('fullpath');
currentDir = fileparts(currentFile);
path_config = fullfile(currentDir, 'config', 'paths.yaml');
path = ReadYaml(path_config);
addpath(genpath(path.yaml_matlab));
currentFile = mfilename('fullpath');


input_path = path.processing_data_dir;
output_path = fullfile(script_dir, 'output', 'backtest_results'); 

% 添加工具路径
addpath(fullfile(script_dir, 'Optimizer_matlab','utils'));

addpath(fullfile(script_dir, 'Optimizer_matlab','tools'));

addpath(fullfile(currentDir, 'Optimizer_matlab'));	

fprintf('=== 批量回测 ===\n');

% 读取配置文件获取投资组合列表
try
    [portfolio_info, ~, ~] = ConfigReader_sql();
    fprintf('找到 %d 个投资组合\n', height(portfolio_info));
catch ME
    fprintf('读取配置文件失败: %s\n', ME.message);
    return;
end

user_names = portfolio_info.user_name;
pythonExe = path.python_exe;





% 循环处理每个投资组合
for i = 1:height(portfolio_info)
    try
        % 获取当前投资组合信息
        current_portfolio = portfolio_info(i, :);
        
        % 提取投资组合名称和用户名称
        if iscell(current_portfolio.portfolio_name)
            portfolio_name = current_portfolio.portfolio_name{1};
        else
            portfolio_name = string(current_portfolio.portfolio_name);
        end
        
        if iscell(current_portfolio.user_name)
            user_name = current_portfolio.user_name{1};
        else
            user_name = string(current_portfolio.user_name);
        end
        
        % 提取日期信息
        if iscell(current_portfolio.start_date)
            start_date = current_portfolio.start_date{1};
            end_date = current_portfolio.end_date{1};
        else
            start_date = string(current_portfolio.start_date);
            end_date = string(current_portfolio.end_date);
        end
        
        fprintf('\n=== 回测投资组合 %d/%d: %s (%s) ===\n', i, height(portfolio_info), portfolio_name, user_name);
        
      
        % 创建回测工具箱实例
        bt = BacktestToolbox.BacktestToolbox();
        
        % 设置配置

        bt.setInputPath(input_path);   % 设置输入路径（用于读取投资组合数据）
        bt.setOutputPath(output_path); % 设置输出路径（用于存储回测结果）
        
        % 设置当前投资组合信息
        bt.setCurrentPortfolio(portfolio_name, user_name, start_date, end_date);
        
        % 运行回测
        fprintf('开始回测...\n');
        bt.runBacktest();
        
        
    catch ME
        fprintf('✗ 回测失败: %s\n', ME.message);
    end
end


if iscell(user_names)
    uname = user_names{1};
elseif isstring(user_names)
    
    uname = char(user_names(1));
elseif ischar(user_names)
    uname = user_names;
else
    
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

try
    netScript = fullfile(currentDir, 'import_netvalue_to_mysql.py');

    table_name = [uname '_backtest'];


    base_folder = fullfile(currentDir, 'output', 'backtest_results', uname);

    cmd2 = sprintf('"%s" "%s" "%s" "%s" "%s"', pythonExe, netScript, table_name, sid, base_folder);
    [status2, cmdout2] = system(cmd2);
    fprintf_log('Netvalue Python return status: %d', status2);
    if ~isempty(cmdout2)
        fprintf_log('Netvalue Python output:\n%s', cmdout2);
    end
    if status2 ~= 0
        fprintf_log('Running netvalue python script failed (status=%d).', status2);
    end
    
catch ME2
    if isprop(ME2, 'identifier') && ~isempty(ME2.identifier)
        id2 = ME2.identifier;
    else
        id2 = 'run_optimizer:netvalueImportFail';
    end
    fprintf_log(id2, 'Failed to launch netvalue importer: %s', ME2.message);
end


fprintf('\n=== 批量回测完成 ===\n');
