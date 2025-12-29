function [df_st, df_stockuniverse] = StableData()

    dbc = DatabaseConnector();
    conn = dbc.openConnection();
    cleanup_conn = onCleanup(@() close(conn));

    query1 = 'SELECT * FROM data_prepared_new.st_stock';
    query2 = 'SELECT * FROM data_prepared_new.stockuniverse WHERE type = ''stockuni_new''';

    try
        df_st = fetch(conn, query1);
        df_stockuniverse = fetch(conn, query2);
    catch ME
        error('StableData:DatabaseError', '获取稳定数据失败: %s', ME.message);
    end
    clear cleanup_conn;
end