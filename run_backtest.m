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
currentDir = fileparts(currentFile);


path = ReadYaml(path_config);

input_path = path.processing_data_dir;
output_path = fullfile(script_dir, 'output', 'backtest_results'); 

% 添加工具路径
addpath(fullfile(script_dir, 'Optimizer_matlab','utils'));

addpath(fullfile(script_dir, 'Optimizer_matlab','tools'));
% addpath(fullfile(script_dir, 'data'));
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
        fprintf('✓ 回测完成\n');
        
    catch ME
        fprintf('✗ 回测失败: %s\n', ME.message);
    end
end

fprintf('\n=== 批量回测完成 ===\n');
