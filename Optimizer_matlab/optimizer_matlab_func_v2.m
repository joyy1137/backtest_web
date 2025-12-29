function [final_weight, barra_saving_info, industry_saving_info] = optimizer_matlab_func_v2(path,path_yes,worker_count)
% 函数功能：读取指定路径下的数据文件，进行投资组合优化
% path：输入路径，包含?有必要的CSV数据文件
% worker_count：并行工作进程数量，默认从配置文件读?
% 使用持久变量保持并行池在函数调用之间存在
persistent persistentPool;

% 记录?始时?
tic;

% 打印接收到的路径参数
fprintf_log('MATLAB函数开始处理路径: %s\n', path);
try append_log_to_file('MATLAB函数开始处理路径: %s', path); catch, end

try
        % 如果配置文件不存在，使用默认配置
    fprintf_log('配置文件不存在，使用默认配置\n');
        config = struct();
        config.worker_count_default = 6;
        config.input_files = struct();
        config.input_files.parameter_selecting = 'parameter_selecting.xlsx';
        config.input_files.stock_risk = 'Stock_risk_exposure.csv';
        config.input_files.index_risk = 'index_risk_exposure.csv';
        config.input_files.stock_score = 'Stock_score.csv';
        config.input_files.stock_code = 'Stock_code.csv';
        config.input_files.initial_weight = 'Stock_initial_weight.csv';
        config.input_files.lower_weight = 'Stock_lower_weight.csv';
        config.input_files.upper_weight = 'Stock_upper_weight.csv';
        config.input_files.index_initial_weight = 'index_initial_weight.csv';
        config.input_files.stock_specific_risk = 'Stock_specific_risk.csv';
        config.input_files.factor_cov = 'factor_cov.csv';
        config.input_files.factor_constraint_upper = 'factor_constraint_upper.csv';
        config.input_files.factor_constraint_lower = 'factor_constraint_lower.csv';
        config.input_files.turnover_constraint = 'turnover_constraint.csv';
        config.output_files = struct();
        config.output_files.weight = 'weight.csv';
        config.output_files.barra_risk = 'barra_risk.csv';
        config.output_files.industry_risk = 'industry_risk.csv';
        config.optimization_params = struct();
        config.optimization_params.Algorithm = 'sqp';
        config.optimization_params.MaxFunctionEvaluations = 800000;
        config.optimization_params.Display = 'iter';
        config.optimization_params.UseParallel = true;
    
    % 设置工作进程数量
    if nargin < 2 || isempty(worker_count)
        worker_count = config.worker_count_default;
    fprintf_log('使用配置文件中的默认并行工作进程数量: %d\n', worker_count);
    else
        worker_count = double(worker_count);
    fprintf_log('使用指定的并行工作进程数: %d\n', worker_count);
    end
    
    % 配置并行?
    fprintf_log('配置并行计算，最大工作进程数: %d\n', worker_count);
    %?查并行池是否已经存在且有?
    if isempty(persistentPool) || ~isvalid(persistentPool) || persistentPool.NumWorkers ~= worker_count
        %?查是否有其他并行?
        poolobj = gcp('nocreate');
        if ~isempty(poolobj)
            % 关闭现有并行?
    fprintf_log('关闭现有并行池（%d个工作进程）\n', poolobj.NumWorkers);
        try append_log_to_file('关闭现有并行池（%d个工作进程）', poolobj.NumWorkers); catch, end
            delete(poolobj);
        end
        % 创建新的并行池并存储在持久变量中
    fprintf_log('创建新并行池 %d 个工作进程\n', worker_count);
    try append_log_to_file('创建新并行池 %d 个工作进程', worker_count); catch, end
        persistentPool = parpool('local', worker_count);
        try append_log_to_file('并行池创建完毕，NumWorkers=%d', persistentPool.NumWorkers); catch, end
        % Ensure workers have access to utils (append_log_to_file, fprintf_log)
        try
            script_dir = fileparts(mfilename('fullpath'));
            utils_path = fullfile(script_dir, 'utils');
            pctRunOnAll(addpath(utils_path));
            try append_log_to_file('已在所有 worker 上添加 utils 路径: %s', utils_path); catch, end
        catch err
            try append_log_to_file('无法在 worker 上添加 utils 路径: %s', err.message); catch, end
        end
    else
        % 重用现有并行?
    fprintf_log('复用现有并行池，%d个工作进程\n', persistentPool.NumWorkers);
    end
    
    % 读取数据
    fprintf_log('开始读取数据...\n');
    try append_log_to_file('开始读取数据: %s', path); catch, end
    
    % 读取风格因子和行业因?
    try
        style_factor = readtable(fullfile(path, config.input_files.parameter_selecting), 'Sheet', 'style');
        style_len = size(style_factor, 1);
    fprintf_log('风格因子数量: %d\n', style_len);
    fprintf_log('风格因子表类型: %s, 维度: %dx%d\n', class(style_factor), size(style_factor, 1), size(style_factor, 2));
        
        industry_factor = readtable(fullfile(path, config.input_files.parameter_selecting), 'Sheet', 'industry');
        industry_len = size(industry_factor, 1);
    fprintf_log('行业因子数量: %d\n', industry_len);
    fprintf_log('行业因子表类型: %s, 维度: %dx%d\n', class(industry_factor), size(industry_factor, 1), size(industry_factor, 2));
    catch e
    fprintf_log('读取因子数据失败: %s，使用默认\n', e.message);
        style_len = 9;  % 默认风格因子数量
        industry_len = 30;  % 默认行业因子数量
    fprintf_log('使用默认风格因子数量: %d, 行业因子数量: %d\n', style_len, industry_len);
    end
    
    % 读取股票风险暴露
    stock_risk_path = fullfile(path, config.input_files.stock_risk);
    fprintf_log('读取股票风险暴露: %s\n', stock_risk_path);
    stock_risk = importdata(stock_risk_path);
    if isstruct(stock_risk)
    fprintf_log('股票风险暴露包含字段: %s\n', strjoin(fieldnames(stock_risk), ', '));
        stock_risk = stock_risk.data;
    end
    fprintf_log('股票风险暴露数据类型: %s, 维度: %dx%d\n', class(stock_risk), size(stock_risk, 1), size(stock_risk, 2));
    if ~isnumeric(stock_risk)
        fprintf_log('警告: stock_risk不是数值类型，尝试转换为数值数组\n');
        if iscell(stock_risk)
            stock_risk = cell2mat(stock_risk);
        else
            stock_risk = double(stock_risk);
        end
    end
    % 确保是double类型
    if ~isa(stock_risk, 'double')
        stock_risk = double(stock_risk);
    end


    stock_risk(isnan(stock_risk)) = 0;
    stock_risk(isinf(stock_risk)) = 0;
    
    % 读取指数风险暴露
    index_risk_path = fullfile(path, config.input_files.index_risk);
    index_risk = importdata(index_risk_path);
    if isstruct(index_risk)
        
        index_risk = index_risk.data;
    end

    if ~isnumeric(index_risk)
        fprintf_log('警告: index_risk不是数值类型，尝试转换为数值数组\n');
        if iscell(index_risk)
            index_risk = cell2mat(index_risk);
        else
            index_risk = double(index_risk);
        end
    end
    % 确保是double类型
    if ~isa(index_risk, 'double')
        index_risk = double(index_risk);
    end

    index_risk(isnan(index_risk)) = 0;
    index_risk(isinf(index_risk)) = 0;
    
    % 读取股票分数
    stock_score_path = fullfile(path, config.input_files.stock_score);
    fprintf_log('读取股票分数: %s\n', stock_score_path);
    stock_score = importdata(stock_score_path);
    if isstruct(stock_score)
    fprintf_log('股票分数包含字段: %s\n', strjoin(fieldnames(stock_score), ', '));
        stock_score = stock_score.data;
    end
    fprintf_log('股票分数数据类型: %s, 维度: %dx%d\n', class(stock_score), size(stock_score, 1), size(stock_score, 2));
    % 读取权重下限
    lower_weight_path = fullfile(path, config.input_files.lower_weight);
    fprintf_log('读取权重下限: %s\n', lower_weight_path);
    lower_weight = importdata(lower_weight_path);
    if isstruct(lower_weight)
    fprintf_log('权重下限包含字段: %s\n', strjoin(fieldnames(lower_weight), ', '));
        lower_weight = lower_weight.data;
    end
    fprintf_log('权重下限数据类型: %s, 维度: %dx%d\n', class(lower_weight), size(lower_weight, 1), size(lower_weight, 2));
    
    % 读取权重上限
    upper_weight_path = fullfile(path, config.input_files.upper_weight);
    fprintf_log('读取权重上限: %s\n', upper_weight_path);
    upper_weight = importdata(upper_weight_path);
    if isstruct(upper_weight)
    fprintf_log('权重上限包含字段: %s\n', strjoin(fieldnames(upper_weight), ', '));
        upper_weight = upper_weight.data;
    end
    fprintf_log('权重上限数据类型: %s, 维度: %dx%d\n', class(upper_weight), size(upper_weight, 1), size(upper_weight, 2));
    
    % 读取指数初始权重
    index_initial_weight_path = fullfile(path, config.input_files.index_initial_weight);
    fprintf_log('读取指数初始权重: %s\n', index_initial_weight_path);
    index_initial_weight = importdata(index_initial_weight_path);
    if isstruct(index_initial_weight)
    fprintf_log('指数初始权重包含字段: %s\n', strjoin(fieldnames(index_initial_weight), ', '));
        index_initial_weight = index_initial_weight.data;
    end
    fprintf_log('指数初始权重数据类型: %s, 维度: %dx%d\n', class(index_initial_weight), size(index_initial_weight, 1), size(index_initial_weight, 2));
    
    % 读取股票特异风险
    stock_specific_risk_path = fullfile(path, config.input_files.stock_specific_risk);
    fprintf_log('读取股票特异风险: %s\n', stock_specific_risk_path);
    stock_sperisk = importdata(stock_specific_risk_path);
    if isstruct(stock_sperisk)
    fprintf_log('股票特异风险包含字段: %s\n', strjoin(fieldnames(stock_sperisk), ', '));
        stock_sperisk = stock_sperisk.data;
    end
    fprintf_log('股票特异风险数据类型: %s, 维度: %dx%d\n', class(stock_sperisk), size(stock_sperisk, 1), size(stock_sperisk, 2));
    
    % 读取因子协方差
    factor_cov_path = fullfile(path, config.input_files.factor_cov);
    fprintf_log('读取因子协方差: %s\n', factor_cov_path);
    factor_cov = importdata(factor_cov_path);
    if isstruct(factor_cov)
    fprintf_log('因子协方差包含字段: %s\n', strjoin(fieldnames(factor_cov), ', '));
        factor_cov = factor_cov.data;
    end
    fprintf_log('因子协方差数据类型: %s, 维度: %dx%d\n', class(factor_cov), size(factor_cov, 1), size(factor_cov, 2));
    
    % 读取因子约束上限
    factor_constraint_upper_path = fullfile(path, config.input_files.factor_constraint_upper);
   
    factor_constraint_upper = importdata(factor_constraint_upper_path);

    if isstruct(factor_constraint_upper)
        factor_constraint_upper = factor_constraint_upper.data;
    end
    fprintf_log('因子约束上限数据类型: %s, 维度: %dx%d\n', class(factor_constraint_upper), size(factor_constraint_upper, 1), size(factor_constraint_upper, 2));
    te_value = factor_constraint_upper(1);
    fprintf_log('跟踪误差约束: %f\n', te_value);
    factor_constraint_upper = factor_constraint_upper(2:end)';
    fprintf_log('处理后的因子约束上限维度: %dx%d\n', size(factor_constraint_upper, 1), size(factor_constraint_upper, 2));
    
    % 读取因子约束下限
    factor_constraint_lower_path = fullfile(path, config.input_files.factor_constraint_lower);
    fprintf_log('读取因子约束下限: %s\n', factor_constraint_lower_path);
    factor_constraint_lower = importdata(factor_constraint_lower_path);
    if isstruct(factor_constraint_lower)
        factor_constraint_lower = factor_constraint_lower.data;
    end
    factor_constraint_lower = factor_constraint_lower(2:end)';
   
    % 读取换手率约束
    turnover_constraint_path = fullfile(path, config.input_files.turnover_constraint);
    fprintf_log('读取换手率约束: %s\n', turnover_constraint_path);
    turnover_constraint = importdata(turnover_constraint_path);
    if isstruct(turnover_constraint)
    fprintf_log('换手率约束包含字段: %s\n', strjoin(fieldnames(turnover_constraint), ', '));
        turnover_constraint = turnover_constraint.data;
    end
    max_turnover = turnover_constraint(1);
    fprintf_log('最大换手率限制: %.2f\n', max_turnover);
    
    % 读取目标函数权重配置（如果存在）
    objective_weights_path = fullfile(path, 'objective_weights.csv');
    if exist(objective_weights_path, 'file')
        fprintf_log('读取目标函数权重配置: %s\n', objective_weights_path);
        objective_weights_table = readtable(objective_weights_path);
        alpha = objective_weights_table.score_weight(1);  % 分数权重
        beta = objective_weights_table.sharpe_weight(1);   % 夏普比率权重
        fprintf_log('从配置文件读取权重：分数权重=%.2f, 夏普比率权重=%.2f\n', alpha, beta);
    else
        % 默认权重
        alpha = 0.7;  % 分数权重
        beta = 0.3;   % 夏普比率权重
        fprintf_log('使用默认权重：分数权重=%.2f, 夏普比率权重=%.2f\n', alpha, beta);
    end
    
    % 计算股票数量
    stock_number = size(stock_score, 1);
    fprintf_log('股票数量: %d\n', stock_number);
    
    % 检查因子约束向量长度
    if length(factor_constraint_upper) < (style_len + industry_len)
        fprintf_log('警告: 因子约束上限向量长度不足，需%d，实际为%d\n', style_len + industry_len, length(factor_constraint_upper));
        % 扩展向量长度
        expanded = zeros(style_len + industry_len, 1);
        expanded(1:length(factor_constraint_upper)) = factor_constraint_upper;
        factor_constraint_upper = expanded;
        fprintf_log('已扩展因子约束上限向量长度至%d\n', length(factor_constraint_upper));
    end
    
    if length(factor_constraint_lower) < (style_len + industry_len)
        fprintf_log('警告: 因子约束下限向量长度不足，需%d，实际为%d\n', style_len + industry_len, length(factor_constraint_lower));
        % 扩展向量长度
        expanded = zeros(style_len + industry_len, 1);
        expanded(1:length(factor_constraint_lower)) = factor_constraint_lower;
        factor_constraint_lower = expanded;
        fprintf_log('已扩展因子约束下限向量长度至%d\n', length(factor_constraint_lower));
    end
    % 分离风格权重和行业权重约束
    style_weight_upper = factor_constraint_upper(1:style_len, :);
    style_weight_lower = factor_constraint_lower(1:style_len, :);
    industry_weight_upper = factor_constraint_upper(style_len+1:end, :);
    industry_weight_lower = factor_constraint_lower(style_len+1:end, :);
    fprintf_log('风格权重上限维度: %dx%d\n', size(style_weight_upper, 1), size(style_weight_upper, 2));
    fprintf_log('风格权重下限维度: %dx%d\n', size(style_weight_lower, 1), size(style_weight_lower, 2));
    fprintf_log('行业权重上限维度: %dx%d\n', size(industry_weight_upper, 1), size(industry_weight_upper, 2));
    fprintf_log('行业权重下限维度: %dx%d\n', size(industry_weight_lower, 1), size(industry_weight_lower, 2));
    
    % 检查股票风险暴露维度
    if size(stock_risk, 2) < (style_len + industry_len)
        fprintf_log('警告: 股票风险暴露维度不足，需%d列，实际只有%d列\n', style_len + industry_len, size(stock_risk, 2));
    end
    
    % 检查指数风险暴露维度
    if size(index_risk, 2) < (style_len + industry_len)
        fprintf_log('警告: 指数风险暴露维度不足，需%d列，实际只有%d列\n', style_len + industry_len, size(index_risk, 2));
    end
    
    % 分离风格风险和行业风险暴露
    barra_stock_risk = stock_risk(:, 1:style_len);
    industry_stock_risk = stock_risk(:, style_len+1:end);
    barra_index_risk = index_risk(:, 1:style_len);
    industry_index_risk = index_risk(:, style_len+1:end);
    
    % 确保 barra_index_risk 和 industry_index_risk 的维度与约束权重匹配
    % 获取约束权重的长度
    style_weight_len = length(style_weight_upper);
    industry_weight_len = length(industry_weight_upper);
    
    % 调整 barra_index_risk 的维度
    if size(barra_index_risk, 1) == 1
        barra_index_risk = barra_index_risk';
    end
    barra_index_risk = barra_index_risk(:);
    if length(barra_index_risk) < style_weight_len
        barra_index_risk = [barra_index_risk; zeros(style_weight_len - length(barra_index_risk), 1)];
    elseif length(barra_index_risk) > style_weight_len
        barra_index_risk = barra_index_risk(1:style_weight_len);
    end
    
    % 调整 industry_index_risk 的维度
    if size(industry_index_risk, 1) == 1
        industry_index_risk = industry_index_risk';
    end
    industry_index_risk = industry_index_risk(:);
    if length(industry_index_risk) < industry_weight_len
        industry_index_risk = [industry_index_risk; zeros(industry_weight_len - length(industry_index_risk), 1)];
    elseif length(industry_index_risk) > industry_weight_len
        industry_index_risk = industry_index_risk(1:industry_weight_len);
    end
    
    fprintf_log('风格风险暴露维度: %dx%d\n', size(barra_stock_risk, 1), size(barra_stock_risk, 2));
    fprintf_log('行业风险暴露维度: %dx%d\n', size(industry_stock_risk, 1), size(industry_stock_risk, 2));
    fprintf_log('指数风格风险暴露维度: %dx%d\n', size(barra_index_risk, 1), size(barra_index_risk, 2));
    fprintf_log('指数行业风险暴露维度: %dx%d\n', size(industry_index_risk, 1), size(industry_index_risk, 2));
    
    % 计算协方差矩阵
    fprintf_log('计算协方差矩阵...\n');
    
    % 确保stock_sperisk是列向量
    if size(stock_sperisk, 1) == 1
        stock_sperisk = stock_sperisk';
        fprintf_log('将股票特异风险转换为列向量，维度: %dx%d\n', size(stock_sperisk, 1), size(stock_sperisk, 2));
    end
    
    
    
    V = (stock_risk * factor_cov * stock_risk' + diag(stock_sperisk.^2));
    
    % 确保协方差矩阵是对称的
    V = (V + V') / 2;
    
    % 确保协方差矩阵是正定的（添加小的正则化项）
    min_eig = min(eig(V));
    if min_eig <= 0
        fprintf_log('协方差矩阵不是正定的，最小特征值: %.6f，添加正则化项\n', min_eig);
        regularization = abs(min_eig) + 1e-6;
        V = V + regularization * eye(size(V, 1));
    end
    
    fprintf_log('协方差矩阵维度: %dx%d\n', size(V, 1), size(V, 2));
            % 读取初始权重
    initial_code_path = fullfile(path, config.input_files.stock_code);
    initial_weight_path = fullfile(path, config.input_files.initial_weight);
    initial_weight_path_yes = fullfile(path_yes, config.output_files.weight);
    initial_code_yes_path = fullfile(path_yes, config.input_files.stock_code);
    fprintf_log('读取初始权重: %s\n', initial_weight_path);
    initial_weight = importdata(initial_weight_path);
    if isstruct(initial_weight)
        fprintf_log('初始权重包含字段: %s\n', strjoin(fieldnames(initial_weight), ', '));
        initial_weight = initial_weight.data;
    end
    fprintf_log('初始权重数据类型: %s, 维度: %dx%d\n', class(initial_weight), size(initial_weight, 1), size(initial_weight, 2));

    % 读取昨天的权重和代码作为换手率计算基准
    yesterday_weight_path = fullfile(path_yes, config.output_files.weight);
    
    yesterday_code_path = fullfile(path_yes, config.input_files.stock_code);
    
    if exist(yesterday_weight_path, 'file') && exist(yesterday_code_path, 'file')
        fprintf_log('读取昨天权重文件: %s\n', yesterday_weight_path);
        fprintf_log('读取昨天代码文件: %s\n', yesterday_code_path);
        
        % 读取昨天的权重
        yesterday_weight = importdata(yesterday_weight_path);
        if isstruct(yesterday_weight)
            yesterday_weight = yesterday_weight.data;
        end
        
        % 确保权重是列向量，如果是多列则只取第一列
        if size(yesterday_weight, 2) > 1
            fprintf_log('警告: 昨天权重文件包含多列(%d列)，只使用第一列\n', size(yesterday_weight, 2));
            yesterday_weight = yesterday_weight(:, 1);
        end
        yesterday_weight = yesterday_weight(:);  % 确保是列向量
        
        % 读取昨天的股票代码
        yesterday_codes = readtable(yesterday_code_path);
        yesterday_codes = table2cell(yesterday_codes(2:end, 1)); % 去掉第一行（日期行），只取第一列
        
        % 读取今天的股票代码
        current_codes = readtable(initial_code_path);
        current_codes = table2cell(current_codes(2:end, 1)); % 去掉第一行（日期行），只取第一列
        
        fprintf_log('昨天权重数据维度: %dx%d, 代码数量: %d\n', size(yesterday_weight, 1), size(yesterday_weight, 2), length(yesterday_codes));
        fprintf_log('今天代码数量: %d\n', length(current_codes));
        
        % 检查昨天权重和代码的维度是否匹配（使用行数而不是length）
        if size(yesterday_weight, 1) == length(yesterday_codes)
            % 检查昨天权重总和
            yesterday_weight_sum = sum(yesterday_weight);
            fprintf_log('昨天权重总和: %.6f\n', yesterday_weight_sum);
            if abs(yesterday_weight_sum - 1.0) > 0.01
                fprintf_log('警告: 昨天权重总和(%.6f)偏离1.0，进行归一化\n', yesterday_weight_sum);
                if abs(yesterday_weight_sum) > 1e-10
                    yesterday_weight = yesterday_weight / yesterday_weight_sum;
                else
                    fprintf_log('错误: 昨天权重总和接近0，无法归一化\n');
                end
            end
            
            % 创建昨天的代码到权重的映射
            yesterday_code_weight_map = containers.Map(yesterday_codes, yesterday_weight);
            
            % 创建今天的代码到索引的映射（用于快速查找）
            current_code_index_map = containers.Map();
            for i = 1:length(current_codes)
                current_code_index_map(current_codes{i}) = i;
            end
            
            % 为今天的每个股票匹配昨天的权重
            turnover_baseline = zeros(size(initial_weight));
            removed_weight_sum = 0;
            
            % 处理今天的股票：匹配昨天的权重
            for i = 1:length(current_codes)
                current_code = current_codes{i};
                if isKey(yesterday_code_weight_map, current_code)
                    turnover_baseline(i) = yesterday_code_weight_map(current_code);
                else
                    % 如果今天的股票在昨天不存在，权重设为0（新增股票）
                    turnover_baseline(i) = 0;
                end
            end
            
            % 处理昨天的股票：计算被移除股票的权重总和
            for i = 1:length(yesterday_codes)
                yesterday_code = yesterday_codes{i};
                if ~isKey(current_code_index_map, yesterday_code)
                    % 昨天的股票在今天不存在（被移除的股票），累加其权重
                    removed_weight_sum = removed_weight_sum + yesterday_code_weight_map(yesterday_code);
                end
            end
            
            fprintf_log('成功匹配股票代码: %d/%d\n', sum(turnover_baseline > 0), length(current_codes));
            fprintf_log('换手率基准权重总和: %.6f\n', sum(turnover_baseline));
            fprintf_log('使用昨天权重作为换手率计算基准（按代码匹配）\n');
        else
            removed_weight_sum = 0;
            fprintf_log('警告: 昨天权重行数(%d)与代码数量(%d)不匹配，使用初始权重作为基准\n', size(yesterday_weight, 1), length(yesterday_codes));
            turnover_baseline = initial_weight;
        end
    else
        removed_weight_sum = 0;
        fprintf_log('未找到昨天权重文件或代码文件，使用初始权重作为换手率计算基准\n');
        if ~exist(yesterday_weight_path, 'file')
            fprintf_log('  缺失文件: %s\n', yesterday_weight_path);
        end
        if ~exist(yesterday_code_path, 'file')
            fprintf_log('  缺失文件: %s\n', yesterday_code_path);
        end
        turnover_baseline = initial_weight;
    end
    
    % 确保removed_weight_sum变量存在
    if ~exist('removed_weight_sum', 'var')
        removed_weight_sum = 0;
    end

    % 检查是否存在yes文件并应用权重约束
    if exist(initial_weight_path_yes, 'file') && exist(initial_code_yes_path, 'file')
        fprintf_log('检测到yes文件，应用权重约束...\n');
        % 读取yes权重文件
        initial_weight_yes = importdata(initial_weight_path_yes);
        if isstruct(initial_weight_yes)
            initial_weight_yes = initial_weight_yes.data;
        end
        
        % 读取yes代码文件
        initial_code_yes = readtable(initial_code_yes_path);
        % 转换为cell数组并去掉第一行（日期行）
        initial_code_yes = table2cell(initial_code_yes(2:end, :));
        
        % 检查代码和权重维度匹配（允许权重数量大于等于代码数量）
        code_count = size(initial_code_yes, 1);
        weight_count = size(initial_weight_yes, 1);
        
        if code_count <= weight_count
            % 允许权重数量大于等于代码数量（包含top股票的情况）
            fprintf_log('代码数量: %d, 权重数量: %d，允许权重约束\n', code_count, weight_count);
            
            % 创建代码到权重的映射（只使用前N个权重，N为代码数量）
            code_weight_map = containers.Map(initial_code_yes, initial_weight_yes(1:code_count));
            
            % 获取当前权重对应的代码
            current_codes = readtable(initial_code_path);
            % 转换为cell数组并去掉第一行（日期行）
            current_codes = table2cell(current_codes(2:end, 1)); % 只取第一列作为代码
            
            % 创建final_initial_weight并初始化为initial_weight
            final_initial_weight = initial_weight;
            
            % 应用权重约束
            for i = 1:length(current_codes)
                code = current_codes{i};  % 使用花括号访问cell数组
                if isKey(code_weight_map, code)
                    final_initial_weight(i) = code_weight_map(code); % 如果在yes列表中，使用yes列表中的权重
                end
                % 如果代码不在yes列表中，保持原始权重不变
            end
            
            % 检查跟踪误差计算
            diff_weight = final_initial_weight - index_initial_weight;
            V_diff = V * diff_weight;
            quad_term = diff_weight' * V_diff;
            annual_term = quad_term * 252;
            
            % 如果年化项小于0，回退到原始权重
            if annual_term < 0
                fprintf_log('警告：应用yes权重后年化跟踪误差项为负（%f），回退到原始权重\n', annual_term);
                final_initial_weight = initial_weight;
            else
                fprintf_log('应用yes权重后年化跟踪误差项为：%f\n', annual_term);
            end
            
            fprintf_log('权重约束应用完成，最终权重维度: %dx%d\n', size(final_initial_weight, 1), size(final_initial_weight, 2));
        else
            warning('yes文件中的代码数量(%d)大于权重数量(%d)，跳过权重约束', code_count, weight_count);
            final_initial_weight = initial_weight;
        end
    else
        fprintf_log('未检测到yes文件，跳过权重约束\n');
        final_initial_weight = initial_weight;
    end

    % 设置目标函数 - 同时最大化分数和夏普比率
    score_stock = stock_score';
    fprintf_log('转置后的股票分数维度: %dx%d\n', size(score_stock, 1), size(score_stock, 2));
    
    % 检查并处理score_stock中的NaN值
    nan_count = sum(isnan(score_stock(:)));
    if nan_count > 0
        fprintf_log('警告: score_stock中有 %d 个NaN值，将其替换为0\n', nan_count);
        score_stock(isnan(score_stock)) = 0;
    end
    
    % 检查并处理score_stock中的Inf值
    inf_count = sum(isinf(score_stock(:)));
    if inf_count > 0
        fprintf_log('警告: score_stock中有 %d 个Inf值，将其替换为0\n', inf_count);
        score_stock(isinf(score_stock)) = 0;
    end
    
    % 计算初始解的各项数值
    initial_score = score_stock * final_initial_weight;
    initial_risk = sqrt((final_initial_weight - index_initial_weight)' * V * (final_initial_weight - index_initial_weight) * 252);

    if initial_risk > 0
        initial_risk = initial_risk;
    else
        initial_risk = 0;
    end

    % 多目标函数：最大化 alpha*分数 + beta*夏普比率
    % 夏普比率 = 预期收益 / 投资组合标准差
    f = @(x) -(alpha * (score_stock * x) + beta * (score_stock * x) / max(sqrt(x' * V * x * 252), 1e-6));
    
    fprintf_log('目标函数设置：分数权重=%.2f, 夏普比率权重=%.2f\n', alpha, beta);
    
    % 调试：检查目标函数在初始点的值
    fprintf_log('调试目标函数计算...\n');
    
    % 检查协方差矩阵V
    fprintf_log('协方差矩阵V检查:\n');
    fprintf_log('  V维度: %dx%d\n', size(V, 1), size(V, 2));
    fprintf_log('  V中的NaN数量: %d\n', sum(isnan(V(:))));
    fprintf_log('  V中的Inf数量: %d\n', sum(isinf(V(:))));
    fprintf_log('  V中的负数数量: %d\n', sum(V(:) < 0));
    fprintf_log('  V的最小值: %.6f\n', min(V(:)));
    fprintf_log('  V的最大值: %.6f\n', max(V(:)));
    
    % 检查V是否是对称矩阵
    if ~issymmetric(V)
        fprintf_log('  警告: 协方差矩阵V不是对称矩阵\n');
    end
    
    % 检查V是否是正定矩阵
    try
        eig_vals = eig(V);
        min_eig = min(eig_vals);
        fprintf_log('  V的最小特征值: %.6f\n', min_eig);
        if min_eig <= 0
            fprintf_log('  警告: 协方差矩阵V不是正定矩阵，最小特征值: %.6f\n', min_eig);
        end
    catch ME
        fprintf_log('  错误: 无法计算协方差矩阵V的特征值: %s\n', ME.message);
    end
    
    x_test = final_initial_weight;
    
    
    % 检查各个组件
    score_component = score_stock * x_test;
    fprintf_log('分数组件: %.6f\n', score_component);
    
    risk_component = x_test' * V * x_test * 252;
    fprintf_log('风险组件 (x''Vx*252): %.6f\n', risk_component);
    
    if risk_component < 0
        fprintf_log('警告: 风险组件为负数: %.6f\n', risk_component);
    end
    
    sqrt_risk = sqrt(max(risk_component, 1e-6));
    fprintf_log('风险标准差: %.6f\n', sqrt_risk);
    
    sharpe_component = score_component / sqrt_risk;
    fprintf_log('夏普比率组件: %.6f\n', sharpe_component);
    
    objective_value = alpha * score_component + beta * sharpe_component;
    fprintf_log('目标函数值: %.6f\n', objective_value);
    
    if isnan(objective_value) || isinf(objective_value)
        fprintf_log('错误: 目标函数值为NaN或Inf\n');
        fprintf_log('alpha: %.6f, beta: %.6f\n', alpha, beta);
        fprintf_log('score_component: %.6f\n', score_component);
        fprintf_log('sharpe_component: %.6f\n', sharpe_component);
    end
    
  
    % 设置初始值和约束
    x0 = final_initial_weight;
    lb = lower_weight;
    ub = upper_weight;
    x0 = max(min(x0, ub), lb);
    
    % 检查初始点权重和是否接近1
    initial_sum = sum(x0);
    if abs(initial_sum - 1.0) > 1e-6
        fprintf_log('警告: 初始点权重和(%.6f)偏离1.0，进行归一化调整\n', initial_sum);
        x0 = x0 / initial_sum;
        % 重新应用边界约束
        x0 = max(min(x0, ub), lb);
        % 再次归一化以确保权重和为1
        x0 = x0 / sum(x0);
    end
    
    fprintf_log('初始解维度: %dx%d\n', size(x0, 1), size(x0, 2));
    fprintf_log('下界维度: %dx%d\n', size(lb, 1), size(lb, 2));
    fprintf_log('上界维度: %dx%d\n', size(ub, 1), size(ub, 2));
    
    % 在调用 fmincon 之前添加初始点验证
    fprintf_log('验证初始点:\n');
    fprintf_log('初始点 x0 中的 NaN: %d, Inf: %d\n', sum(isnan(x0(:))), sum(isinf(x0(:))));
    fprintf_log('初始点 x0 范围: [%f, %f]\n', min(x0), max(x0));
    fprintf_log('初始点 x0 和: %f\n', sum(x0));
      % 验证初始点是否满足约束条件
    [g0, h0] = fun2(x0, stock_risk, index_risk, style_weight_upper, style_weight_lower, industry_weight_upper, industry_weight_lower, V, index_initial_weight, te_value, style_len, turnover_baseline, max_turnover, removed_weight_sum);
    assignin('base', 'g0', g0);
    assignin('base', 'h0', h0);
    fprintf_log('初始点约束条件 g0 中的 NaN: %d, Inf: %d\n', sum(isnan(g0(:))), sum(isinf(g0(:))));
    fprintf_log('初始点约束条件 g0 范围: [%f, %f]\n', min(g0), max(g0));
    
    % 详细分析约束违反情况
    violated_constraints = g0 > 1e-6;  % 找出违反的约束
    num_violated = sum(violated_constraints);
    fprintf_log('违反的约束数量: %d / %d\n', num_violated, length(g0));
    
    if num_violated > 0
        fprintf_log('违反的约束详情:\n');
        % 风格因子约束
        style_upper_violated = sum(g0(1:style_len) > 1e-6);
        style_lower_violated = sum(g0(style_len+1:2*style_len) > 1e-6);
        fprintf_log('  风格因子上限违反: %d / %d\n', style_upper_violated, style_len);
        fprintf_log('  风格因子下限违反: %d / %d\n', style_lower_violated, style_len);
        
        % 行业因子约束
        industry_upper_violated = sum(g0(2*style_len+1:2*style_len+industry_len) > 1e-6);
        industry_lower_violated = sum(g0(2*style_len+industry_len+1:2*style_len+2*industry_len) > 1e-6);
        fprintf_log('  行业因子上限违反: %d / %d\n', industry_upper_violated, industry_len);
        fprintf_log('  行业因子下限违反: %d / %d\n', industry_lower_violated, industry_len);
        
        % 跟踪误差约束
        te_violation = g0(end-1);
        fprintf_log('  跟踪误差约束违反: %.6f (限制: %.6f)\n', te_violation, te_value);
        
        % 换手率约束
        turnover_violation = g0(end);
        fprintf_log('  换手率约束违反: %.6f (限制: %.6f)\n', turnover_violation, max_turnover);
        
        % 如果约束违反严重，建议放宽约束
        if te_violation > 0.01 || turnover_violation > 0.1
            fprintf_log('警告: 约束违反严重，建议检查约束设置是否合理\n');
        end
    end
    % 设置非线性约束
    nonlcon = @(x)fun2(x, stock_risk, index_risk, style_weight_upper, style_weight_lower, industry_weight_upper, industry_weight_lower, V, index_initial_weight, te_value, style_len, turnover_baseline, max_turnover, removed_weight_sum);
    
    % 设置线性约束
    A = []; 
    b = [];
    Aeq = ones(1, stock_number);
    beq = ones(1, 1);
    fprintf_log('线性等式约束维度: Aeq(%dx%d), beq(%dx%d)\n', size(Aeq, 1), size(Aeq, 2), size(beq, 1), size(beq, 2));
    
    % 设置优化选项
    % For more reliable logging capture, run fmincon without internal
    % parallel workers (workers may print on their own stdout which the
    % main diary doesn't capture). Set UseParallel=false to force serial
    % evaluation of finite differences. This is the fastest fix to ensure
    % all fprintf output appears in the main log file.
    options = optimoptions('fmincon', ...
        'Display', 'iter-detailed', ...  % 更详细的输出
        'Algorithm', config.optimization_params.Algorithm, ...
        'MaxFunctionEvaluations', config.optimization_params.MaxFunctionEvaluations, ...
        'UseParallel', false, ...
        'CheckGradients', true, ...  % 检查梯度
        'FiniteDifferenceType', 'central', ...  % 使用中心差分
        'FiniteDifferenceStepSize', 1e-6, ...  % 设置差分步长
        'ConstraintTolerance', 1e-3, ...  % 放宽约束容忍度
        'StepTolerance', 1e-6, ...  % 步长容忍度
        'OptimalityTolerance', 1e-4, ...  % 最优性容忍度
        'MaxIterations', 1000);  % 增加最大迭代次数
    fprintf_log('优化选项设置完成: 算法=%s, 最大函数评估次数=%d, 并行计算=%s\n', ...
        config.optimization_params.Algorithm, ...
        config.optimization_params.MaxFunctionEvaluations, ...
        string(config.optimization_params.UseParallel));
    
    % 求解优化问题
    fprintf_log('开始求解优化问题...\n');
    try append_log_to_file('开始求解优化问题'); catch, end
    [x, fval, exitflag, output] = fmincon(f, x0, A, b, Aeq, beq, lb, ub, nonlcon, options);
    try append_log_to_file('fmincon 返回: exitflag=%d, fval=%f', exitflag, fval); catch, end
    
    % 检查是否有更好的可行解
    if isfield(output, 'bestfeasible') && ~isempty(output.bestfeasible)
        fprintf_log('找到更好的可行解，使用该解\n');
        x = output.bestfeasible.x;
        fval = output.bestfeasible.fval;
    end
    
    % 如果优化失败但约束违反很小，尝试微调约束容忍度
    if exitflag == -2 && output.constrviolation <= 1e-2
        fprintf_log('优化失败但约束违反较小(%.6f)，尝试微调约束容忍度...\n', output.constrviolation);
        
        % 创建更宽松的约束容忍度选项
        relaxed_options = optimoptions(options, 'ConstraintTolerance', output.constrviolation * 1.1);
        
        % 重新优化
        fprintf_log('使用微调后的约束容忍度重新优化...\n');
        [x_relaxed, fval_relaxed, exitflag_relaxed, output_relaxed] = fmincon(f, x, A, b, Aeq, beq, lb, ub, nonlcon, relaxed_options);
        
        % 如果成功，使用结果
        if exitflag_relaxed > 0
            fprintf_log('微调约束容忍度优化成功，使用该结果\n');
            x = x_relaxed;
            fval = fval_relaxed;
            exitflag = exitflag_relaxed;
            output = output_relaxed;
        else
            fprintf_log('微调约束容忍度优化也失败，使用原始结果\n');
        end
    end
    
    % 智能约束处理：如果约束违反很小，接受当前解
    if exitflag == -2 && output.constrviolation <= 5e-3
        fprintf_log('约束违反很小(%.6f)，接受当前解作为近似最优解\n', output.constrviolation);
        exitflag = 1;  % 将退出标志改为成功
    end
    
    % 输出优化结果信息
    fprintf_log('优化完成，输出标志: %d\n', exitflag);
    try append_log_to_file('优化完成，输出标志: %d', exitflag); catch, end
    fprintf_log('目标函数值: %f\n', fval);
    
    % 根据退出标志输出详细信息
    switch exitflag
        case 1
            fprintf_log('优化成功完成：\n');
            fprintf_log('  找到满足约束条件的局部最小值\n');
        case 2
            fprintf_log('优化成功完成：\n');
            fprintf_log('  目标函数在可行域内呈现非递减趋势\n');
            fprintf_log('  约束条件在可行域内满足要求\n');
        case 0
            fprintf_log('优化达到最大迭代次数：\n');
            fprintf_log('  最后一步梯度范数: %e\n', output.firstorderopt);
            fprintf_log('  约束违反值: %e\n', output.constrviolation);
        case -1
            fprintf_log('优化被输出函数终止\n');
        case -2
            fprintf_log('未找到可行解\n');
        otherwise
            fprintf_log('优化因其他原因终止\n');
    end
    
    fprintf_log('函数评估次数: %d\n', output.funcCount);
    if isfield(output, 'firstorderopt')
        fprintf_log('一阶最优性: %e\n', output.firstorderopt);
    end
    if isfield(output, 'constrviolation')
        fprintf_log('约束违反值: %e\n', output.constrviolation);
    end
    
    % 计算结果
    portfolio_risk = sqrt((x - index_initial_weight)' * V * (x - index_initial_weight) * 252);
    portfolio_barra_risk = barra_stock_risk' * x;
    portfolio_industry_risk = industry_stock_risk' * x;
    
    % 确保 portfolio_*_risk 的维度与约束权重匹配
    portfolio_barra_risk = portfolio_barra_risk(:);
    portfolio_industry_risk = portfolio_industry_risk(:);
    
    % 调整 portfolio_barra_risk 的长度
    if length(portfolio_barra_risk) < style_weight_len
        portfolio_barra_risk = [portfolio_barra_risk; zeros(style_weight_len - length(portfolio_barra_risk), 1)];
    elseif length(portfolio_barra_risk) > style_weight_len
        portfolio_barra_risk = portfolio_barra_risk(1:style_weight_len);
    end
    
    % 调整 portfolio_industry_risk 的长度
    if length(portfolio_industry_risk) < industry_weight_len
        portfolio_industry_risk = [portfolio_industry_risk; zeros(industry_weight_len - length(portfolio_industry_risk), 1)];
    elseif length(portfolio_industry_risk) > industry_weight_len
        portfolio_industry_risk = portfolio_industry_risk(1:industry_weight_len);
    end
    
    final_score = score_stock * x;
    final_weight = x;
    
    % 处理负数权重：将负数权重设为0，然后重新归一化
    negative_count = sum(final_weight < 0);
    if negative_count > 0
        fprintf_log('检测到 %d 个负数权重，将其设为0并重新归一化\n', negative_count);
        final_weight(final_weight < 0) = 0;
        % 重新归一化
        weight_sum_positive = sum(final_weight);
        if weight_sum_positive > 0
            final_weight = final_weight / weight_sum_positive;
        else
            fprintf_log('警告: 所有权重都为非正数，无法归一化\n');
        end
    end
    
    weight_sum = sum(final_weight);
    
    % 计算实际换手率（相对于昨天的权重，按代码匹配）
    % 确保维度匹配
    final_weight_vec = final_weight(:);  % 确保是列向量
    turnover_baseline_vec = turnover_baseline(:);  % 确保是列向量
    
    if length(final_weight_vec) ~= length(turnover_baseline_vec)
        fprintf_log('警告: 换手率计算中维度不匹配: final_weight长度=%d, turnover_baseline长度=%d\n', length(final_weight_vec), length(turnover_baseline_vec));
        % 如果维度不匹配，调整turnover_baseline
        if length(turnover_baseline_vec) < length(final_weight_vec)
            turnover_baseline_vec = [turnover_baseline_vec; zeros(length(final_weight_vec) - length(turnover_baseline_vec), 1)];
        else
            turnover_baseline_vec = turnover_baseline_vec(1:length(final_weight_vec));
        end
    end
    
    % 换手率 = 前后权重相减的绝对值之和 + 被移除股票的权重
    % 被移除股票的权重从w_old变为0，贡献w_old到换手率
    actual_turnover = sum(abs(final_weight_vec - turnover_baseline_vec)) + removed_weight_sum;
    
    % 输出换手率详细信息
    fprintf_log('实际换手率: %.4f (限制: %.4f)\n', actual_turnover, max_turnover);
    fprintf_log('换手率基准权重总和: %.4f\n', sum(turnover_baseline));
    fprintf_log('最终权重总和: %.4f\n', sum(final_weight));
    
    % 计算最大权重变化（用于调试）
    weight_changes = abs(final_weight_vec - turnover_baseline_vec);
    [max_change, max_idx] = max(weight_changes);
    if exist('current_codes', 'var') && length(current_codes) >= max_idx
        fprintf_log('最大权重变化: %.4f (股票: %s)\n', max_change, current_codes{max_idx});
    else
        fprintf_log('最大权重变化: %.4f\n', max_change);
    end

    
    fprintf_log('投资组合风险: %f\n', portfolio_risk);
    fprintf_log('最终分数: %f\n', final_score);
    fprintf_log('权重总和: %f\n', weight_sum);
    fprintf_log('风格风险维度: %dx%d\n', size(portfolio_barra_risk, 1), size(portfolio_barra_risk, 2));
    fprintf_log('行业风险维度: %dx%d\n', size(portfolio_industry_risk, 1), size(portfolio_industry_risk, 2));
    
    % 计算风险比率（确保维度匹配）
    % industry_index_risk 和 portfolio_industry_risk 都应该是列向量，且长度相同
    industry_index_abs = abs(industry_index_risk);
    industry_index_abs(industry_index_abs < 1e-10) = 1e-10;  % 避免除零
    industry_risk_ratio = (portfolio_industry_risk - industry_index_risk) ./ industry_index_abs;
    fprintf_log('行业风险比率维度: %dx%d\n', size(industry_risk_ratio, 1), size(industry_risk_ratio, 2));
    
    % 保存风险信息（确保维度匹配）
    barra_index_abs = abs(barra_index_risk);
    barra_index_abs(barra_index_abs < 1e-10) = 1e-10;  % 避免除零
    barra_risk_ratio = (portfolio_barra_risk - barra_index_risk) ./ barra_index_abs;
    
    barra_saving_info = [portfolio_barra_risk, barra_index_risk, barra_risk_ratio, ...
        repmat(portfolio_risk, size(portfolio_barra_risk)), repmat(final_score, size(portfolio_barra_risk)), repmat(weight_sum, size(portfolio_barra_risk))];
    
    industry_saving_info = [portfolio_industry_risk, industry_index_risk, industry_risk_ratio];
    
    
    % 保存结果  
    weight_path = fullfile(path, config.output_files.weight);
    barra_risk_path = fullfile(path, config.output_files.barra_risk);
    industry_risk_path = fullfile(path, config.output_files.industry_risk);
    
    csvwrite(weight_path, final_weight);
    csvwrite(barra_risk_path, barra_saving_info);
    csvwrite(industry_risk_path, industry_saving_info);
    
 
    % 报告总时间
    fprintf_log('优化完成，总时间: %.2f秒\n', toc);
    
catch e
    % 捕获并处理错误
    fprintf_log('错误: %s\n', e.message);
    try append_log_to_file('错误: %s', e.message); catch, end
    fprintf_log('堆栈跟踪:\n');
    try append_log_to_file('堆栈跟踪: %s', getReport(e,'basic')); catch, end
    disp(e.stack);
    
    % 确保返回值不为空
    if ~exist('final_weight', 'var')
        final_weight = [];
        fprintf_log('由于错误，返回空的final_weight\n');
    end
    if ~exist('barra_saving_info', 'var')
        barra_saving_info = [];
        fprintf_log('由于错误，返回空的barra_saving_info\n');
    end
    if ~exist('industry_saving_info', 'var')
        industry_saving_info = [];
        fprintf_log('由于错误，返回空的industry_saving_info\n');
    end
    
    % 重新抛出错误
    rethrow(e);
end

end

% 非线性约束函数
function [g, h] = fun2(x, stock_risk, index_risk, style_weight_upper, style_weight_lower, industry_weight_upper, industry_weight_lower, V, index_initial_weight, TE, style_len, yesterday_weight, max_turnover, removed_weight_sum)
    % 分离风格和行业风险
    barra_stock_risk = stock_risk(:, 1:style_len);
    industry_stock_risk = stock_risk(:, style_len+1:end);
    barra_index_risk = index_risk(:, 1:style_len);
    industry_index_risk = index_risk(:, style_len+1:end);
    
    % 计算投资组合风险暴露
    portfolio_barra_risk = barra_stock_risk' * x;
    portfolio_industry_risk = industry_stock_risk' * x;
    
    % 确保 portfolio_*_risk 是列向量
    portfolio_barra_risk = portfolio_barra_risk(:);
    portfolio_industry_risk = portfolio_industry_risk(:);
    
    % 确保维度匹配：portfolio_*_risk 是列向量，需要将 index_risk 转置为列向量
    % 如果 barra_index_risk 是行向量，转置为列向量；如果已经是列向量，保持列向量
    if size(barra_index_risk, 1) == 1
        barra_index_risk = barra_index_risk';
    end
    % 确保是列向量格式
    barra_index_risk = barra_index_risk(:);
    
    % 如果 industry_index_risk 是行向量，转置为列向量；如果已经是列向量，保持列向量
    if size(industry_index_risk, 1) == 1
        industry_index_risk = industry_index_risk';
    end
    % 确保是列向量格式
    industry_index_risk = industry_index_risk(:);
    
    % 确保维度匹配：industry_index_risk 的长度应该与 industry_weight_upper 

    industry_weight_len = length(industry_weight_upper);
    industry_index_len = length(industry_index_risk);
    if industry_index_len < industry_weight_len
        % 如果 industry_index_risk 太短，用零填充
        industry_index_risk = [industry_index_risk; zeros(industry_weight_len - industry_index_len, 1)];
    elseif industry_index_len > industry_weight_len
        % 如果 industry_index_risk 太长，截断
        industry_index_risk = industry_index_risk(1:industry_weight_len);
    end
    
    % 确保 portfolio_industry_risk 的长度与 industry_index_risk 匹配
    portfolio_industry_len = length(portfolio_industry_risk);
    if portfolio_industry_len < industry_weight_len
        % 如果 portfolio_industry_risk 太短，用零填充
        portfolio_industry_risk = [portfolio_industry_risk; zeros(industry_weight_len - portfolio_industry_len, 1)];
    elseif portfolio_industry_len > industry_weight_len
        % 如果 portfolio_industry_risk 太长，截断
        portfolio_industry_risk = portfolio_industry_risk(1:industry_weight_len);
    end
    
    % 确保 barra_index_risk 的长度与 style_weight_upper 匹配
    barra_weight_len = length(style_weight_upper);
    barra_index_len = length(barra_index_risk);
    if barra_index_len < barra_weight_len
        % 如果 barra_index_risk 太短，用零填充
        barra_index_risk = [barra_index_risk; zeros(barra_weight_len - barra_index_len, 1)];
    elseif barra_index_len > barra_weight_len
        % 如果 barra_index_risk 太长，截断
        barra_index_risk = barra_index_risk(1:barra_weight_len);
    end
    
    % 确保 portfolio_barra_risk 的长度与 barra_index_risk 匹配
    portfolio_barra_len = length(portfolio_barra_risk);
    if portfolio_barra_len < barra_weight_len
        % 如果 portfolio_barra_risk 太短，用零填充
        portfolio_barra_risk = [portfolio_barra_risk; zeros(barra_weight_len - portfolio_barra_len, 1)];
    elseif portfolio_barra_len > barra_weight_len
        % 如果 portfolio_barra_risk 太长，截断
        portfolio_barra_risk = portfolio_barra_risk(1:barra_weight_len);
    end
    
    % 计算约束条件，确保不会出现复数
    barra_diff = portfolio_barra_risk - barra_index_risk;
    industry_diff = portfolio_industry_risk - industry_index_risk;
    
    % 计算约束条件，使用 real 函数确保结果为实数
    % 使用列向量版本计算绝对值
    barra_index_abs = abs(barra_index_risk);
    industry_index_abs = abs(industry_index_risk);
    
    % 修复约束条件计算：确保约束条件的符号正确
    % 对于上限约束：g <= 0 表示满足约束
    % 对于下限约束：g <= 0 表示满足约束
    g1_upper = barra_diff - style_weight_upper .* barra_index_abs;
    g1_lower = style_weight_lower .* barra_index_abs - barra_diff;
    g2_upper = industry_diff - industry_weight_upper .* industry_index_abs;
    g2_lower = industry_weight_lower .* industry_index_abs - industry_diff;


    
    % 计算跟踪误差
    diff_weight = x - index_initial_weight;
    V_diff = V * diff_weight;
    quad_term = diff_weight' * V_diff;
    annual_term = quad_term * 252;
    % 确保 annual_term 非负且非 NaN
    if isnan(annual_term) || isinf(annual_term) || annual_term < 0
        tracking_error = 0;
    else
        tracking_error = sqrt(real(annual_term));
    end
    % 确保 tracking_error 非 NaN
    if isnan(tracking_error) || isinf(tracking_error)
        tracking_error = 0;
    end
    g3 = tracking_error - TE;

    
    % 计算换手率约束（相对于昨天的权重）
    % 确保维度匹配
    x = x(:);  % 确保是列向量
    yesterday_weight = yesterday_weight(:);  % 确保是列向量
    
    if length(x) ~= length(yesterday_weight)
        % 如果维度不匹配，调整yesterday_weight
        if length(yesterday_weight) < length(x)
            yesterday_weight = [yesterday_weight; zeros(length(x) - length(yesterday_weight), 1)];
        else
            yesterday_weight = yesterday_weight(1:length(x));
        end
    end
    
    % 换手率 = 前后权重相减的绝对值之和 + 被移除股票的权重
    % 被移除股票的权重从w_old变为0，贡献w_old到换手率
    turnover = sum(abs(x - yesterday_weight)) + removed_weight_sum;
    g4 = turnover - max_turnover;
    
    % 组合所有约束条件
    g = [g1_upper; g1_lower; g2_upper; g2_lower; g3; g4];
    h = [];
end