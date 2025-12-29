function stock_weight_constraint = stock_weight_constraint_matlab(df_score, df_index, stock_number, quantiles, up_params, low_params, constraint_mode, portfolio_constraint, portfolio_name, df_st)

% 添加全局错误处理
try



target_stock_number = stock_number; % 默认使用函数参数



if nargin >= 8 && ~isempty(portfolio_constraint) && nargin >= 9 && ~isempty(portfolio_name)
    % 查找配置参数列名
    fn_candidates = {'factor_name','factor_constraint'};
    fn_colname = '';
    for fn = fn_candidates
        if ismember(fn{1}, portfolio_constraint.Properties.VariableNames)
            fn_colname = fn{1};
            break;
        end
    end
    if isempty(fn_colname)
        fn_colname = portfolio_constraint.Properties.VariableNames{1}; 
    end
   
    
    % 获取目标组合列的索引
    portfolio_col_idx = find(strcmp(portfolio_constraint.Properties.VariableNames, portfolio_name));

    if ~isempty(portfolio_col_idx)
           % 获取stock_number
        stock_number_idx = strcmp(portfolio_constraint.(fn_colname), 'stock_number');
        if any(stock_number_idx)
            target_stock_number = str2double(portfolio_constraint{stock_number_idx, portfolio_col_idx});
            if isnan(target_stock_number)
                target_stock_number = stock_number; % 如果读取失败，使用函数参数
            end
        end
        
        
        % 获取top_number
        top_number_idx = strcmp(portfolio_constraint.(fn_colname), 'top_number');
        if any(top_number_idx)
            top_number = str2double(portfolio_constraint{top_number_idx, portfolio_col_idx});
            if isnan(top_number)
                top_number = 0;
            end
        end
  
        
        % 获取top_weight_upper
        top_weight_upper_idx = strcmp(portfolio_constraint.(fn_colname), 'top_up');
        if any(top_weight_upper_idx)
            top_weight_upper = str2double(portfolio_constraint{top_weight_upper_idx, portfolio_col_idx});
            if isnan(top_weight_upper)
                top_weight_upper = 1.0;
                fprintf_log('警告：top_weight_upper参数读取失败，使用默认值%.2f\n', top_weight_upper);
            end
        end
        
        % 获取top_weight_lower
        top_weight_lower_idx = strcmp(portfolio_constraint.(fn_colname), 'top_lo');
        if any(top_weight_lower_idx)
            top_weight_lower = str2double(portfolio_constraint{top_weight_lower_idx, portfolio_col_idx});
            if isnan(top_weight_lower)
                top_weight_lower = 0.8;
                fprintf_log('警告：top_weight_lower参数读取失败，使用默认值%.2f\n', top_weight_lower);
            end
        end
    end
end

% 添加数据完整性检查
if isempty(df_score)
    error('df_score为空，无法继续处理');
end
if isempty(df_index)
    error('df_index为空，无法继续处理');
end
% 检查ST股票数据可用性

% 1. 处理整个股票池中的top股票
df_top = table();
if top_number > 0
    
    df_score_filtered = df_score;
    
    % 安全地处理ST股票数据
    if nargin >= 10 && ~isempty(df_st) && ismember('code', df_st.Properties.VariableNames)
        try
            st_codes = df_st.code;
            % 过滤掉ST股票
            is_st_stock = ismember(df_score_filtered.code, st_codes);
            df_score_filtered = df_score_filtered(~is_st_stock, :);
        catch ME
            % 跳过ST股票过滤，使用原始数据
        end
    end
    
 
    
    % 按分数降序排列
    try
        df_score_filtered = sortrows(df_score_filtered, 'final_score', 'descend');
    catch ME
        error('无法按分数排序，程序终止');
    end
    
    % 取前top_number只股票
    if height(df_score_filtered) >= top_number
        try
            df_top = df_score_filtered(1:top_number, :);
        catch ME
            error('无法选择top股票，程序终止');
        end
        % 计算top股票权重
        df_top.top_weight = ones(height(df_top), 1) / height(df_top);
        
        % 根据constraint_mode计算top股票权重约束
        if nargin < 7 || isempty(constraint_mode)
            mode = 'v1'; % 默认v1
        else
            mode = lower(constraint_mode); % 转换为小写
        end
        
        switch mode
            case 'v1'
                % v1模式：相对约束
                df_top.weight_upper_top = df_top.top_weight * top_weight_upper;
                df_top.weight_lower_top = df_top.top_weight * top_weight_lower;
                df_top.initial_weight_top = df_top.top_weight; % 使用基础权重作为初始权重
            case 'v2'
                % v2模式：绝对约束
                df_top.weight_upper_top = df_top.top_weight + top_weight_upper;
                df_top.weight_lower_top = df_top.top_weight + top_weight_lower;
                df_top.initial_weight_top = df_top.top_weight; % 使用基础权重作为初始权重
            otherwise
                error('Unknown constraint_mode: %s', constraint_mode);
        end
        % 只保留需要的列
        df_top = df_top(:, {'code', 'weight_upper_top', 'weight_lower_top', 'initial_weight_top', 'final_score'});
        
    end
end

% 2. 处理指数成分股权重
df_weight = df_index;
if ~ismember('weight', df_weight.Properties.VariableNames)
    df_weight.weight = zeros(height(df_weight), 1);
end
df_weight.Properties.VariableNames{'weight'} = 'initial_weight_index';

% 3. 合并分数和指数权重
T = outerjoin(df_score, df_weight, 'Keys', 'code', 'MergeKeys', true);
T = sortrows(T, 'final_score', 'descend');
if ~ismember('initial_weight_index', T.Properties.VariableNames)
    T.initial_weight_index = zeros(height(T),1);
end

% 2. 分组
score_vec = T.final_score;
N = height(T);
qv = quantile(score_vec, quantiles);
group_idx = zeros(N,1);
for k = 1:length(quantiles)-1
    group_idx(score_vec >= qv(k+1) & score_vec < qv(k)) = k;
end
% 最高分那组
group_idx(score_vec >= qv(1)) = 1;
T.group = group_idx;

% 3. 计算上下界
T.weight_upper_index = zeros(N,1);
T.weight_lower_index = zeros(N,1);
for k = 1:length(up_params)
    idx = (T.group == k);
    if nargin < 7 || isempty(constraint_mode)
        mode = 'v1'; % 默认v1
    fprintf_log('警告：constraint_mode未指定，使用默认v1模式\n');
    else
        mode = lower(constraint_mode); % 转换为小写
    end
    switch mode
        case 'v1'
            T.weight_upper_index(idx) = T.initial_weight_index(idx) .* (1 + up_params(k));
            T.weight_lower_index(idx) = T.initial_weight_index(idx) .* (1 - low_params(k));
        case 'v2' 
            T.weight_upper_index(idx) = T.initial_weight_index(idx) + up_params(k);
            T.weight_lower_index(idx) = T.initial_weight_index(idx) - low_params(k);
        otherwise
            error('Unknown constraint_mode: %s', mode);
    end
end
T.weight_upper_index(T.weight_upper_index<0) = 0;
T.weight_lower_index(T.weight_lower_index<0) = 0;

% 4. 处理top股票数据并合并
% 始终为T表添加top相关列，即使没有top股票
T.weight_upper_top = zeros(height(T), 1);
T.weight_lower_top = zeros(height(T), 1);
T.initial_weight_top = zeros(height(T), 1);
T.is_top_stock = false(height(T), 1); % 添加标识列

if ~isempty(df_top)
    % 为top股票赋值
    for i = 1:height(df_top)
        top_code = df_top.code{i};
        idx = strcmp(T.code, top_code);
        if any(idx)
            T.weight_upper_top(idx) = df_top.weight_upper_top(i);
            T.weight_lower_top(idx) = df_top.weight_lower_top(i);
            T.initial_weight_top(idx) = df_top.initial_weight_top(i);
            T.is_top_stock(idx) = true; % 标记为top股票
        else
            % 如果top股票不在T中，需要添加
            new_row = table();
            new_row.code = {top_code};
            new_row.final_score = df_top.final_score(i);
            new_row.initial_weight_index = 0;
            new_row.group = 1; % 默认分组
            new_row.weight_upper_index = 0;
            new_row.weight_lower_index = 0;
            new_row.weight_upper_top = df_top.weight_upper_top(i);
            new_row.weight_lower_top = df_top.weight_lower_top(i);
            new_row.initial_weight_top = df_top.initial_weight_top(i);
            new_row.is_top_stock = true; % 标记为top股票
            T = [T; new_row];
        end
    end
end

% 计算最终权重界限（无论是否有top股票）
T.weight_upper = zeros(height(T), 1);
T.weight_lower = zeros(height(T), 1);
for i = 1:height(T)
    if T.is_top_stock(i) && T.initial_weight_index(i) > 0
        % 既是指数成分股又是top股票
        T.weight_upper(i) = T.weight_upper_top(i) + T.weight_upper_index(i);
        T.weight_lower(i) = min(T.weight_lower_top(i), T.weight_lower_index(i));
    elseif T.is_top_stock(i)
        % 只是top股票，不是指数成分股
        T.weight_upper(i) = T.weight_upper_top(i);
        T.weight_lower(i) = T.weight_lower_top(i);
    elseif T.initial_weight_index(i) > 0
        % 只是指数成分股，不是top股票，只用百分位限制
        T.weight_upper(i) = T.weight_upper_index(i);
        T.weight_lower(i) = T.weight_lower_index(i);
    else
        % 其他情况
        T.weight_upper(i) = 0;
        T.weight_lower(i) = 0;
    end
end
T.initial_weight = T.initial_weight_top + T.initial_weight_index;

% 5. 最关键的修改：只保留指数成分股和top股票，并过滤ST股
% 创建筛选后的结果表，只包含指数成分股和top股票
index_codes = df_index.code;
top_codes = {};
if ~isempty(df_top)
    top_codes = df_top.code;
end

% 过滤指数成分股中的ST股票
if nargin >= 10 && ~isempty(df_st)
    st_codes = df_st.code;
    % 从指数成分股中移除ST股票
    is_index_st = ismember(index_codes, st_codes);
    index_codes_filtered = index_codes(~is_index_st);
    
else
    % 如果没有ST数据，使用原始指数成分股
    index_codes_filtered = index_codes;
    fprintf_log('警告：没有ST股票数据，指数成分股未进行ST过滤\n');
end

% 合并所有需要保留的股票代码（已过滤ST的指数成分股 + top股票）
keep_codes = [index_codes_filtered; top_codes];
keep_idx = ismember(T.code, keep_codes);

% 只保留需要的股票
T_filtered = T(keep_idx, :);

% 6. 检查是否需要补充股票到target_stock_number
current_count = height(T_filtered);
if current_count < target_stock_number
    need_additional = target_stock_number - current_count;
    
    % 从剩余的高分股票中选择（排除已选中的股票和ST股票）
    already_selected_codes = T_filtered.code;
    
    % 创建候选股票池：排除已选股票和ST股票
    candidate_mask = ~ismember(T.code, already_selected_codes);
    if nargin >= 10 && ~isempty(df_st)
        st_codes = df_st.code;
        candidate_mask = candidate_mask & ~ismember(T.code, st_codes);
    end
    
    T_candidates = T(candidate_mask, :);
    T_candidates = sortrows(T_candidates, 'final_score', 'descend');
    
    if height(T_candidates) >= need_additional
        % 选择需要的数量
        T_additional = T_candidates(1:need_additional, :);
        
        % 为补充股票设置top股票权重约束
        additional_count = height(T_additional);
        % 计算补充股票的平均权重
        additional_weight = ones(additional_count, 1) / additional_count;
        
        % 根据constraint_mode为补充股票设置权重约束
        if nargin < 7 || isempty(constraint_mode)
            mode = 'v1'; % 默认v1
        else
            mode = lower(constraint_mode); % 转换为小写
        end
        
        switch mode
            case 'v1'
                % v1模式：相对约束
                T_additional.weight_upper_top = additional_weight * top_weight_upper;
                T_additional.weight_lower_top = additional_weight * top_weight_lower;
                T_additional.initial_weight_top = additional_weight; % 使用基础权重作为初始权重
            case 'v2'
                % v2模式：绝对约束
                T_additional.weight_upper_top = additional_weight + top_weight_upper;
                T_additional.weight_lower_top = additional_weight + top_weight_lower;
                T_additional.initial_weight_top = additional_weight; % 使用基础权重作为初始权重
            otherwise
                error('Unknown constraint_mode: %s', constraint_mode);
        end
        T_additional.is_top_stock = false(height(T_additional), 1); % 标记为补充股票
        
        % 清零指数权重（补充股票不是指数成分股）
        T_additional.weight_upper_index = zeros(height(T_additional), 1);
        T_additional.weight_lower_index = zeros(height(T_additional), 1);
        T_additional.initial_weight_index = zeros(height(T_additional), 1);
        
        % 重新计算最终权重界限
        T_additional.weight_upper = zeros(height(T_additional), 1);
        T_additional.weight_lower = zeros(height(T_additional), 1);
        for i = 1:height(T_additional)
            if T_additional.is_top_stock(i) && T_additional.initial_weight_index(i) > 0
                % 既是指数成分股又是top股票
                T_additional.weight_upper(i) = T_additional.weight_upper_top(i) + T_additional.weight_upper_index(i);
                T_additional.weight_lower(i) = min(T_additional.weight_lower_top(i), T_additional.weight_lower_index(i));
            elseif T_additional.is_top_stock(i)
                % 只是top股票，不是指数成分股
                T_additional.weight_upper(i) = T_additional.weight_upper_top(i);
                T_additional.weight_lower(i) = T_additional.weight_lower_top(i);
            elseif T_additional.initial_weight_index(i) > 0
                % 只是指数成分股，不是top股票，只用百分位限制
                T_additional.weight_upper(i) = T_additional.weight_upper_index(i);
                T_additional.weight_lower(i) = T_additional.weight_lower_index(i);
            else
                % 其他情况
                T_additional.weight_upper(i) = 0;
                T_additional.weight_lower(i) = 0;
            end
        end
        T_additional.initial_weight = T_additional.initial_weight_top + T_additional.initial_weight_index;
        
        % 检查并修复可能的NaN值
        T_additional.weight_upper(isnan(T_additional.weight_upper)) = 0;
        T_additional.weight_lower(isnan(T_additional.weight_lower)) = 0;
        T_additional.initial_weight(isnan(T_additional.initial_weight)) = 0;
        
        % 合并到最终结果
        T_filtered = [T_filtered; T_additional];
        
    else
        if height(T_candidates) > 0
            T_additional = T_candidates;
            additional_count = height(T_additional);
            
            % 为所有可用的候选股票设置top股票权重约束
            additional_weight = ones(additional_count, 1) / additional_count;
            
            % 根据constraint_mode为补充股票设置权重约束
            if nargin < 7 || isempty(constraint_mode)
                mode = 'v1'; % 默认v1
            else
                mode = lower(constraint_mode); % 转换为小写
            end
            
            switch mode
                case 'v1'
                    % v1模式：相对约束
                    T_additional.weight_upper_top = additional_weight * top_weight_upper;
                    T_additional.weight_lower_top = additional_weight * top_weight_lower;
                    T_additional.initial_weight_top = additional_weight; % 使用基础权重作为初始权重
                case 'v2'
                    % v2模式：绝对约束
                    T_additional.weight_upper_top = additional_weight + top_weight_upper;
                    T_additional.weight_lower_top = additional_weight + top_weight_lower;
                    T_additional.initial_weight_top = additional_weight; % 使用基础权重作为初始权重
                otherwise
                    error('Unknown constraint_mode: %s', constraint_mode);
            end
            T_additional.is_top_stock = false(height(T_additional), 1);
            
            % 清零指数权重
            T_additional.weight_upper_index = zeros(height(T_additional), 1);
            T_additional.weight_lower_index = zeros(height(T_additional), 1);
            T_additional.initial_weight_index = zeros(height(T_additional), 1);
            
            % 重新计算最终权重界限
            T_additional.weight_upper = zeros(height(T_additional), 1);
            T_additional.weight_lower = zeros(height(T_additional), 1);
            for i = 1:height(T_additional)
                if T_additional.is_top_stock(i) && T_additional.initial_weight_index(i) > 0
                    % 既是指数成分股又是top股票
                    T_additional.weight_upper(i) = T_additional.weight_upper_top(i) + T_additional.weight_upper_index(i);
                    T_additional.weight_lower(i) = min(T_additional.weight_lower_top(i), T_additional.weight_lower_index(i));
                elseif T_additional.is_top_stock(i)
                    % 只是top股票，不是指数成分股
                    T_additional.weight_upper(i) = T_additional.weight_upper_top(i);
                    T_additional.weight_lower(i) = T_additional.weight_lower_top(i);
                elseif T_additional.initial_weight_index(i) > 0
                    % 只是指数成分股，不是top股票，只用百分位限制
                    T_additional.weight_upper(i) = T_additional.weight_upper_index(i);
                    T_additional.weight_lower(i) = T_additional.weight_lower_index(i);
                else
                    % 其他情况
                    T_additional.weight_upper(i) = 0;
                    T_additional.weight_lower(i) = 0;
                end
            end
            T_additional.initial_weight = T_additional.initial_weight_top + T_additional.initial_weight_index;
            
            % 检查并修复可能的NaN值
            T_additional.weight_upper(isnan(T_additional.weight_upper)) = 0;
            T_additional.weight_lower(isnan(T_additional.weight_lower)) = 0;
            T_additional.initial_weight(isnan(T_additional.initial_weight)) = 0;
            
            T_filtered = [T_filtered; T_additional];
        end
    end
end

% 按code排序，导出最终结果
T_filtered = sortrows(T_filtered, 'code');

% 最终检查并修复所有可能的NaN值
nan_upper = sum(isnan(T_filtered.weight_upper));
nan_lower = sum(isnan(T_filtered.weight_lower)); 
nan_initial = sum(isnan(T_filtered.initial_weight));

if nan_upper > 0 || nan_lower > 0 || nan_initial > 0
    % 修复NaN值
    T_filtered.weight_upper(isnan(T_filtered.weight_upper)) = 0;
    T_filtered.weight_lower(isnan(T_filtered.weight_lower)) = 0;
    T_filtered.initial_weight(isnan(T_filtered.initial_weight)) = 0;
end

% 输出最终股票信息
final_count = height(T_filtered);
index_count = sum(T_filtered.initial_weight_index > 0);
top_count = sum(T_filtered.is_top_stock); % 使用标识列统计top股票数量
additional_count = final_count - index_count - top_count; % 补充的高分股票数量
total_index_original = height(df_index);
total_index_after_st_filter = length(index_codes_filtered);

fprintf_log('目标股票数量: %d\n', target_stock_number);
fprintf_log('最终股票数量: %d (其中指数成分股: %d, top股票: %d, 补充股票: %d)\n', final_count, index_count, top_count, additional_count);

stock_weight_constraint = T_filtered;

catch ME
    % 错误处理
    rethrow(ME);
end

end
