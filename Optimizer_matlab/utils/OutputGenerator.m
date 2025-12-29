function OutputGenerator(outdir, data_date, upper_tbl, lower_tbl, style_list, industry_list, ...
    portfolio_constraint, info_row, turnover_val, score_weight, sharpe_weight, ...
    code_list, score_vec, init_weight, df_score, df_index, stock_number, quantiles, ...
    up_params, low_params, constraint_mode, exposure_mat, factor_names, specific_vec, ...
    factor_cov_mat, index_type, df_st)
% OutputGenerator - 输出生成模块
% 生成所有输出文件
%
% 输入:
%   outdir - 输出目录
%   data_date - 数据日期
%   upper_tbl - 上界约束表
%   lower_tbl - 下界约束表
%   style_list - 风格因子列表
%   industry_list - 行业因子列表
%   portfolio_constraint - 投资组合约束表
%   info_row - 投资组合信息行
%   turnover_val - 换手率约束值
%   score_weight - 得分权重
%   sharpe_weight - 夏普权重
%   code_list - 股票代码列表
%   score_vec - 得分向量
%   init_weight - 初始权重
%   df_score - 得分数据表
%   df_index - 指数数据表
%   stock_number - 股票数量
%   quantiles - 分位数
%   up_params - 上界参数
%   low_params - 下界参数
%   constraint_mode - 约束模式
%   exposure_mat - 风险暴露矩阵
%   factor_names - 因子名称列表
%   specific_vec - 特异性风险向量
%   factor_cov_mat - 因子协方差矩阵
%   index_exposure_data - 指数风险暴露数据
%   index_type - 指数类型

    % 创建输出目录
    if ~exist(outdir, 'dir')
        mkdir(outdir);
    end

    % 导出因子约束文件
    try
        % 转置为两行：第一行为因子名，第二行为约束值
        % 兼容因子上/下界可能为 cell（字符串）的情况，先做安全转换
        upper_names = cellstr(upper_tbl.factor_name);
        lower_names = cellstr(lower_tbl.factor_name);
        % 提取并确保为数值向量
        upper_vals = upper_tbl.upper_bound;
        lower_vals = lower_tbl.lower_bound;
        if iscell(upper_vals)
            upper_vals = cellfun(@(x) str2double(x), upper_vals);
        end
        if iscell(lower_vals)
            lower_vals = cellfun(@(x) str2double(x), lower_vals);
        end
        % 保存为csv
        upper_path = fullfile(outdir, 'factor_constraint_upper.csv');
        lower_path = fullfile(outdir, 'factor_constraint_lower.csv');
        fid = fopen(upper_path, 'w');
        if ~isempty(upper_names)
            fprintf(fid, '%s,', upper_names{1:end-1}); fprintf(fid, '%s\n', upper_names{end});
        end
        if ~isempty(upper_vals)
            if numel(upper_vals) > 1
                fprintf(fid, '%g,', upper_vals(1:end-1)); fprintf(fid, '%g\n', upper_vals(end));
            else
                fprintf(fid, '%g\n', upper_vals);
            end
        end
        fclose(fid);
        fid = fopen(lower_path, 'w');
        if ~isempty(lower_names)
            fprintf(fid, '%s,', lower_names{1:end-1}); fprintf(fid, '%s\n', lower_names{end});
        end
        if ~isempty(lower_vals)
            if numel(lower_vals) > 1
                fprintf(fid, '%g,', lower_vals(1:end-1)); fprintf(fid, '%g\n', lower_vals(end));
            else
                fprintf(fid, '%g\n', lower_vals);
            end
        end
        fclose(fid);
    catch ME
        warning(ME.identifier, '因子约束文件导出失败: %s', ME.message);
    end

    % 导出参数文件
    try
        % 获取portfolio_name
        portfolio_name = info_row.portfolio_name{1};
        
        % 从portfolio_constraint中只提取需要的列

        col_names = portfolio_constraint.Properties.VariableNames;
        factor_col_idx = 1; % 默认第一列
        for i = 1:length(col_names)
            if contains(lower(col_names{i}), {'factor', 'constraint'})
                factor_col_idx = i;
                break;
            end
        end
        
        % 2. 找到与portfolio_name匹配的列
        portfolio_col_idx = find(strcmp(col_names, portfolio_name));
        
        if isempty(portfolio_col_idx)
            error('未找到与投资组合名称 %s 匹配的列', portfolio_name);
        end
        
        % 3. 只提取这两列
        filtered_constraint = portfolio_constraint(:, [factor_col_idx, portfolio_col_idx]);
        param_cell = table2cell(filtered_constraint);
        available_date = data_date;
        
        % 添加额外参数（现在固定为2列）
        extra_params = {
            'portfolio_name', portfolio_name;
            'score_type', info_row.score_type{1};
            'index_type', info_row.index_type{1};
            'mode_type', info_row.mode_type{1};
            'available_date', available_date;
            'user_name', info_row.user_name{1};
        };
        
        param_cell = [param_cell; extra_params];
        param_path = fullfile(outdir, 'parameter_selecting.xlsx');
        writecell(param_cell, param_path, 'Sheet', 'parameters', 'WriteMode', 'overwrite');
        % 只导出一列 style_list 到 style sheet，表头为 factor_name
        % 确保style_list是列向量
        if isrow(style_list)
            style_list = style_list';
        end
        style_cell = [{'factor_name'}; style_list];
        writecell(style_cell, param_path, 'Sheet', 'style', 'WriteMode', 'overwritesheet');
        % 只导出一列 industry_list 到 industry sheet，表头为 factor_name
        % 确保industry_list是列向量
        if isrow(industry_list)
            industry_list = industry_list';
        end
        industry_cell = [{'factor_name'}; industry_list];
        writecell(industry_cell, param_path, 'Sheet', 'industry', 'WriteMode', 'overwritesheet');
    catch ME
        warning(ME.identifier, '参数文件导出失败: %s', ME.message);
    end

    % 导出换手率约束文件
    if ~isempty(turnover_val)
        try
            turnover_path = fullfile(outdir, 'turnover_constraint.csv');
            fid = fopen(turnover_path, 'w');
            fprintf(fid, 'max_turnover\n');
            fprintf(fid, '%s\n', turnover_val);
            fclose(fid);
        catch ME
            warning(ME.identifier, '换手率约束文件导出失败: %s', ME.message);
        end
    end

    % 导出目标权重文件
    if ~isempty(score_weight) && ~isempty(sharpe_weight)
        try
            objw_path = fullfile(outdir, 'objective_weights.csv');
            fid = fopen(objw_path, 'w');
            fprintf(fid, 'score_weight,sharpe_weight\n');
            fprintf(fid, '%s,%s\n', score_weight, sharpe_weight);
            fclose(fid);
        catch ME
            warning(ME.identifier, '目标权重文件导出失败: %s', ME.message);
        end
    end

    % 导出股票权重约束文件并获取更新的股票列表
    try
        portfolio_name = info_row.portfolio_name{1};
        try
            T = stock_weight_constraint_matlab(df_score, df_index, stock_number, quantiles, up_params, low_params, constraint_mode, portfolio_constraint, portfolio_name, df_st);
            
        catch ME
            fprintf_log('股票权重约束函数执行失败: %s\n', ME.message);
            fprintf_log('错误位置: %s (第 %d 行)\n', ME.stack(1).name, ME.stack(1).line);
            rethrow(ME);
        end
        
        % 使用股票权重约束函数返回的股票列表
        updated_code_list = T.code;
        updated_score_vec = zeros(size(updated_code_list));
        updated_init_weight = zeros(size(updated_code_list));
        
        % 直接从T表中获取分数信息
        if ismember('final_score', T.Properties.VariableNames)
            updated_score_vec = T.final_score;
            
        else
            % 如果T表中没有分数信息，使用原来的匹配逻辑
            [~, score_idx] = ismember(string(updated_code_list), string(code_list));
            valid_score_idx = score_idx > 0;
            updated_score_vec(valid_score_idx) = score_vec(score_idx(valid_score_idx));
            fprintf_log('使用匹配逻辑获取股票分数，匹配到 %d/%d 个股票\n', sum(valid_score_idx), length(updated_code_list));
        end
        
        % 匹配初始权重向量
        [~, weight_idx] = ismember(string(updated_code_list), string(code_list));
        valid_weight_idx = weight_idx > 0;
        updated_init_weight(valid_weight_idx) = init_weight(weight_idx(valid_weight_idx));
        
        % 导出股票代码文件
        stock_code_cell = [{data_date}; cellstr(string(updated_code_list))];
        stock_code_path = fullfile(outdir, 'Stock_code.csv');
        writecell(stock_code_cell, stock_code_path);
        
        % 导出股票得分文件
        score_col_name = data_date;
        stock_score_cell = [{score_col_name}; num2cell(updated_score_vec)];
        stock_score_path = fullfile(outdir, 'Stock_score.csv');
        writecell(stock_score_cell, stock_score_path);
        
        % 导出指数初始权重文
        stock_init_weight_cell = [{data_date}; num2cell(updated_init_weight)];
        stock_init_weight_path = fullfile(outdir, 'index_initial_weight.csv');
        writecell(stock_init_weight_cell, stock_init_weight_path);
        
        % 导出股票权重约束文件
        % 使用新的权重列名，兼容旧的列名
        if ismember('initial_weight', T.Properties.VariableNames)
            stock_init_weight_vec = T.initial_weight;
        else
            stock_init_weight_vec = T.weight_lower_index;
        end
        stock_init_weight_cell = [{data_date}; num2cell(stock_init_weight_vec)];
        stock_init_weight_path = fullfile(outdir, 'Stock_initial_weight.csv');
        writecell(stock_init_weight_cell, stock_init_weight_path);

        % 导出 Stock_lower_weight.csv
        if ismember('weight_lower', T.Properties.VariableNames)
            stock_lower_weight_vec = T.weight_lower;
        else
            stock_lower_weight_vec = T.weight_lower_index;
        end
        stock_lower_weight_cell = [{data_date}; num2cell(stock_lower_weight_vec)];
        stock_lower_weight_path = fullfile(outdir, 'Stock_lower_weight.csv');
        writecell(stock_lower_weight_cell, stock_lower_weight_path);

        % 导出 Stock_upper_weight.csv
        if ismember('weight_upper', T.Properties.VariableNames)
            stock_upper_weight_vec = T.weight_upper;
        else
            stock_upper_weight_vec = T.weight_upper_index;
        end
        stock_upper_weight_cell = [{data_date}; num2cell(stock_upper_weight_vec)];
        stock_upper_weight_path = fullfile(outdir, 'Stock_upper_weight.csv');
        writecell(stock_upper_weight_cell, stock_upper_weight_path);
        
        % 更新code_list和相关向量以供后续使用
        code_list = updated_code_list;
        score_vec = updated_score_vec;
        init_weight = updated_init_weight;
        
        % 更新风险暴露矩阵和特异性风险向量以匹配新的股票列表
        if ~isempty(exposure_mat) && size(exposure_mat, 1) ~= length(code_list)
           
            try
                dbc = DatabaseConnector();
                exposure_tbl = dbc.stock_factor_exposure_withdraw(data_date);
                specific_tbl = dbc.factor_risk_withdraw(data_date);
                
                % 重新构建风险暴露矩阵
                [is_in, loc] = ismember(string(code_list), string(exposure_tbl.code));
                factor_names_temp = exposure_tbl.Properties.VariableNames;
                factor_names_temp(strcmp(factor_names_temp, 'code')) = [];
                n_stock = length(code_list);
                n_factor = length(factor_names_temp);
                exposure_mat = nan(n_stock, n_factor);
                exposure_mat(is_in, :) = table2array(exposure_tbl(loc(is_in), factor_names_temp));
                exposure_mat(isnan(exposure_mat)) = 0;
                
                % 重新构建特异性风险向量
                code_names = specific_tbl.Properties.VariableNames;
                code_list_str = cellstr(string(code_list));
                specific_vec = nan(length(code_list_str), 1);
                for ii = 1:length(code_list_str)
                    code = code_list_str{ii};
                    if ismember(code, code_names)
                        specific_vec(ii) = specific_tbl{'specificrisk', code};
                    end
                end
                specific_vec(isnan(specific_vec)) = 0;
                
            catch ME
                warning(ME.identifier, '更新风险数据失败: %s', ME.message);
            end
        end
        
    catch ME
        warning(ME.identifier, '股票权重约束文件导出失败: %s', ME.message);
        
        % 如果出错，尝试从df_score中获取高分股票
    fprintf_log('尝试从df_score中获取高分股票...\n');
        
        % 第一步：获得有效的指数成分股
        if ~isempty(df_score) && ismember('final_score', df_score.Properties.VariableNames)
            % 从df_score中获取指数成分股
            index_stocks = df_score;
            index_codes = index_stocks.code;
            index_scores = index_stocks.final_score;
            index_weights = index_stocks.weight;
            
            fprintf_log('第一步：从df_score中获取了 %d 个指数成分股\n', length(index_codes));
            
            % 第二步：获得有效的top股票
            % 从原始数据中获取所有股票，按分数排序
            all_stocks_data = [code_list, num2cell(score_vec)];
            all_stocks_table = cell2table(all_stocks_data, 'VariableNames', {'code', 'score'});
            all_stocks_sorted = sortrows(all_stocks_table, 'score', 'descend');
            
            % 获取top股票（前top_number个分数最高的股票）
            top_number = min(0, stock_number); 
            top_stocks = all_stocks_sorted(1:min(top_number, height(all_stocks_sorted)), :);
            top_codes = top_stocks.code;
            top_scores = zeros(size(top_codes));
            top_weights = zeros(size(top_codes));
            
            % 为top股票设置分数和权重
            for i = 1:length(top_codes)
                code = top_codes{i};
                idx = find(strcmp(code_list, code));
                if ~isempty(idx)
                    top_scores(i) = score_vec(idx);
                    
                    % 检查top股票是否是指数成分股
                    if ismember(code, index_codes)
                        % 如果是指数成分股，使用指数权重
                        idx_in_index = find(strcmp(index_codes, code));
                        top_weights(i) = index_weights(idx_in_index);
                    else
                        % 如果不是指数成分股，使用1/top_number
                        top_weights(i) = 1 / top_number;
                    end
                end
            end
            
            fprintf_log('第二步：获取了 %d 个top股票\n', length(top_codes));
            
            % 第三步：检查总数是否满足stock_number
            total_stocks = length(index_codes) + length(top_codes);
            fprintf_log('第三步：指数成分股(%d) + top股票(%d) = %d，目标数量: %d\n', ...
                length(index_codes), length(top_codes), total_stocks, stock_number);
            
            if total_stocks >= stock_number
                % 如果总数满足要求，合并股票列表
                updated_code_list = [index_codes; top_codes];
                updated_score_vec = [index_scores; top_scores];
                updated_init_weight = [index_weights; top_weights];
                
                fprintf_log('股票数量满足要求，最终股票数量: %d\n', length(updated_code_list));
            else
                % 第四步：如果不足，从分数高的股票中补充
                remaining_needed = stock_number - total_stocks;
                fprintf_log('第四步：需要补充 %d 个股票\n', remaining_needed);
                
                % 找到不在指数成分股和top股票中的股票
                used_codes = [index_codes; top_codes];
                available_stocks = all_stocks_sorted;
                available_stocks = available_stocks(~ismember(available_stocks.code, used_codes), :);
                
                if height(available_stocks) >= remaining_needed
                    % 补充股票
                    additional_stocks = available_stocks(1:remaining_needed, :);
                    additional_codes = additional_stocks.code;
                    additional_scores = zeros(size(additional_codes));
                    additional_weights = zeros(size(additional_codes));
                    
                    % 为补充股票设置分数和权重
                    for i = 1:length(additional_codes)
                        code = additional_codes{i};
                        idx = find(strcmp(code_list, code));
                        if ~isempty(idx)
                            additional_scores(i) = score_vec(idx);
                            additional_weights(i) = init_weight(idx);
                        end
                    end
                    
                    % 合并所有股票
                    updated_code_list = [index_codes; top_codes; additional_codes];
                    updated_score_vec = [index_scores; top_scores; additional_scores];
                    updated_init_weight = [index_weights; top_weights; additional_weights];
                    
                    fprintf('成功补充了 %d 个股票，最终股票数量: %d\n', remaining_needed, length(updated_code_list));
                else
                    % 如果补充股票不足，使用所有可用的股票
                    updated_code_list = [index_codes; top_codes; available_stocks.code];
                    updated_score_vec = [index_scores; top_scores; zeros(height(available_stocks), 1)];
                    updated_init_weight = [index_weights; top_weights; zeros(height(available_stocks), 1)];
                    
                    fprintf('警告: 补充股票不足，最终股票数量: %d\n', length(updated_code_list));
                end
            end
            
            % 输出最终结果
            fprintf('最终股票数量: %d\n', length(updated_code_list));
            fprintf('前5个最高分股票: ');
            for i = 1:min(5, length(updated_code_list))
                fprintf('%s(%.4f) ', updated_code_list{i}, updated_score_vec(i));
            end
            fprintf('\n');
            
            % 检查top股票中哪些也是指数成分股
            top_in_index = ismember(top_codes, index_codes);
            if sum(top_in_index) > 0
                fprintf('警告: %d 个top股票也是指数成分股，权重约束需要特殊处理\n', sum(top_in_index));
            end
        else
            % 如果df_score不可用，使用原始数据
            updated_code_list = code_list;
            updated_score_vec = score_vec;
            updated_init_weight = init_weight;
            fprintf('df_score不可用，使用原始数据\n');
        end
        
        % 导出股票代码文件
        stock_code_cell = [{data_date}; cellstr(string(updated_code_list))];
        stock_code_path = fullfile(outdir, 'Stock_code.csv');
        writecell(stock_code_cell, stock_code_path);

        % 导出股票得分文件
        score_col_name = data_date;
        stock_score_cell = [{score_col_name}; num2cell(updated_score_vec)];
        stock_score_path = fullfile(outdir, 'Stock_score.csv');
        writecell(stock_score_cell, stock_score_path);

        % 导出指数初始权重文件
        stock_init_weight_cell = [{data_date}; num2cell(init_weight)];
        stock_init_weight_path = fullfile(outdir, 'index_initial_weight.csv');
        writecell(stock_init_weight_cell, stock_init_weight_path);
    end

    % 导出股票风险暴露文件
    if ~isempty(exposure_mat) && ~isempty(factor_names)
        try
            % 确保exposure_mat行数与code_list长度匹配
            if size(exposure_mat, 1) == length(code_list)
                stock_risk_exposure_cell = [factor_names; num2cell(exposure_mat)];
                stock_risk_exposure_path = fullfile(outdir, 'Stock_risk_exposure.csv');
                writecell(stock_risk_exposure_cell, stock_risk_exposure_path);
            else
                fprintf('警告: exposure_mat行数(%d)与code_list长度(%d)不匹配，跳过风险暴露文件导出\n', ...
                    size(exposure_mat, 1), length(code_list));
            end
        catch ME
            warning(ME.identifier, '股票风险暴露文件导出失败: %s', ME.message);
        end
    end

    % 导出特异性风险文件
    if ~isempty(specific_vec)
        try
            % 现在维度应该匹配，因为我们已经更新了risk数据
            code_list_str = cellstr(string(code_list));
            if length(code_list_str) == length(specific_vec)
                stock_specific_risk_cell = [code_list_str'; num2cell(specific_vec)'];
                stock_specific_risk_path = fullfile(outdir, 'Stock_specific_risk.csv');
                writecell(stock_specific_risk_cell, stock_specific_risk_path);
            else
                fprintf('警告: code_list长度(%d)与specific_vec长度(%d)仍不匹配，跳过特异性风险文件导出\n', length(code_list_str), length(specific_vec));
            end
        catch ME
            warning(ME.identifier, '特异性风险文件导出失败: %s', ME.message);
        end
    end

    % 导出股票总得分文件
    try
        df_score_sorted = sortrows(df_score, 'final_score', 'descend');
        stock_total_score_path = fullfile(outdir, 'Stock_total_score.csv');
        writetable(df_score_sorted, stock_total_score_path);
    catch ME
        warning(ME.identifier, '股票总得分文件导出失败: %s', ME.message);
    end

    % 导出因子协方差文件
    if ~isempty(factor_cov_mat) && ~isempty(factor_names)
        try
            cov_cell = [factor_names; num2cell(factor_cov_mat)];
            factor_cov_matrix_path = fullfile(outdir, 'factor_cov.csv');
            writecell(cov_cell, factor_cov_matrix_path);
        catch ME
            warning(ME.identifier, '因子协方差文件导出失败: %s', ME.message);
        end
    end

    % 导出指数风险暴露文件
    try
        dbc = DatabaseConnector();
        style_factors = style_list;
        industry_factors = industry_list;
        factor_names_idx = [style_factors; industry_factors];
        [df_hs300, df_zz500, df_zz1000, df_zz2000, df_zzA500] = dbc.index_exposure_withdraw(data_date);
        switch index_type
            case {'沪深300','hs300'}
                df_idx = df_hs300;
            case {'中证500','zz500'}
                df_idx = df_zz500;
            case {'中证1000','zz1000'}
                df_idx = df_zz1000;
            case {'中证2000','zz2000'}
                df_idx = df_zz2000;
            case {'中证A500','zzA500'}
                df_idx = df_zzA500;
            otherwise
                df_idx = df_hs300;
        end
        [~, col_idx] = ismember(factor_names_idx, df_idx.Properties.VariableNames);
        % 过滤掉无效的索引（col_idx为0表示因子在表中找不到）
        valid_idx = col_idx > 0;
        if ~any(valid_idx)
            warning('所有因子在指数风险暴露表中都找不到，跳过指数风险暴露文件导出');
        else
            valid_col_idx = col_idx(valid_idx);
            valid_factor_names = factor_names_idx(valid_idx);
            row_data = table2array(df_idx(1, valid_col_idx));
            index_risk_path = fullfile(outdir, 'index_risk_exposure.csv');
            fid = fopen(index_risk_path, 'w');
            if length(valid_factor_names) > 1
                fprintf(fid, '%s,', valid_factor_names{1:end-1}); fprintf(fid, '%s\n', valid_factor_names{end});
                fprintf(fid, '%g,', row_data(1:end-1)); fprintf(fid, '%g\n', row_data(end));
            else
                fprintf(fid, '%s\n', valid_factor_names{1});
                fprintf(fid, '%g\n', row_data(1));
            end
            fclose(fid);
        end
    catch ME
        warning(ME.identifier, '指数风险暴露文件导出失败: %s', ME.message);
    end
end

