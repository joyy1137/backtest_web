function [portfolio_weights, portfolio_scores] = get_portfolio_weights(date, inputpath_backtesting)
    % 获取投资组合权重和评分数据
    % 输入:
    %   date - 日期 (datetime格式)
    %   inputpath_backtesting - 回测数据路径
    % 输出:
    %   portfolio_weights - 投资组合权重表 (包含code和weight列)
    %   portfolio_scores - 投资组合评分表 (包含code和score列)
    
    % 转换日期格式
    if isdatetime(date)
        date_str = datestr(date, 'yyyy-mm-dd');
    else
        date_str = char(date);
    end
    
    % 构建文件路径
    date_path = fullfile(inputpath_backtesting, date_str);
    code_file = fullfile(date_path, 'Stock_code.csv');
    weight_file = fullfile(date_path, 'weight.csv');
    score_file = fullfile(date_path, 'Stock_score.csv');
    
   
    
    % 检查文件是否存在
    if ~exist(code_file, 'file') || ~exist(weight_file, 'file') || ~exist(score_file, 'file')
        fprintf('  错误：文件不存在\n');
        error('投资组合数据文件不存在: %s', date_path);
    end
    
    try
        % 读取股票代码（跳过第一行日期）
        df_code = readtable(code_file, 'ReadVariableNames', false, 'HeaderLines', 1);
        if isempty(df_code)
            error('股票代码文件为空');
        end
        
        % 读取权重数据
        df_weight = readtable(weight_file, 'ReadVariableNames', false);
        if isempty(df_weight)
            error('权重文件为空');
        end
        
        % 读取评分数据（跳过第一行日期）
        df_score = readtable(score_file, 'ReadVariableNames', false, 'HeaderLines', 1);
        if isempty(df_score)
            error('评分文件为空');
        end
        
        % 确保三个文件的行数一致
        if height(df_code) ~= height(df_weight) || height(df_code) ~= height(df_score)
            error('股票代码、权重和评分文件行数不一致: 代码文件%d行，权重文件%d行，评分文件%d行', height(df_code), height(df_weight), height(df_score));
        end
        
        % 创建投资组合权重表
        portfolio_weights = table();
        portfolio_weights.code = df_code{:, 1};
        portfolio_weights.weight = df_weight{:, 1};
        
        % 创建投资组合评分表
        portfolio_scores = table();
        portfolio_scores.code = df_code{:, 1};
        portfolio_scores.score = df_score{:, 1};
        
        % 移除空值和无效数据（在标准化之前）
        valid_mask = ~isnan(portfolio_weights.weight) & portfolio_weights.weight > 0;
        portfolio_weights = portfolio_weights(valid_mask, :);
        portfolio_scores = portfolio_scores(valid_mask, :);
        
        
        
        % 检查数据有效性
        if isempty(portfolio_weights)
            error('移除空值后没有有效的投资组合数据');
        end
        
        % 标准化权重
        total_weight = sum(portfolio_weights.weight);
        
        
        if total_weight > 0
            portfolio_weights.weight = portfolio_weights.weight / total_weight;
        else
            error('权重总和为0，数据异常');
        end
        
        
    catch ME
        error('读取投资组合权重数据失败: %s', ME.message);
    end
end
