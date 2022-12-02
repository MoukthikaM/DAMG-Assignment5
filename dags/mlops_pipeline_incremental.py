def incremental_elt(session, 
                    state_dict:dict, 
                    files_to_ingest:list, 
                    download_base_url,
                    use_prestaged=False) -> str:

    from datetime import datetime

    load_stage_name=state_dict['load_stage_name']
    load_table_name=state_dict['load_table_name']
    trips_table_name=state_dict['trips_table_name']
    
    if use_prestaged:
        print("Skipping extract.  Using provided bucket for pre-staged files.")
        
        schema1_download_files = list()
        schema2_download_files = list()
        schema2_start_date = datetime.strptime('202201', "%Y%m")

        for file_name in files_to_ingest:
            file_start_date = datetime.strptime(file_name.split("-")[0], "%Y%m")
            if file_start_date < schema2_start_date:
                schema1_download_files.append(file_name)
            else:
                schema2_download_files.append(file_name)
        
        
        load_stage_names = {'schema1':load_stage_name+'/schema1/', 'schema2':load_stage_name+'/schema2/'}
        files_to_load = {'schema1': schema1_download_files, 'schema2': schema2_download_files}
    else:
        print("Extracting files from public location.")
        load_stage_names = extract_to_stage(session=session, 
                                                                    files_to_download=files_to_ingest, 
                                                                    download_base_url=download_base_url, 
                                                                    load_stage_name=load_stage_name)
        
        files_to_load = extract_to_stage(session=session, 
                                                                    files_to_download=files_to_ingest, 
                                                                    download_base_url=download_base_url, 
                                                                    load_stage_name=load_stage_name)
        
        # files_to_load['schema1']=[file+'.gz' for file in files_to_load['schema1']]
        # files_to_load['schema2']=[file+'.gz' for file in files_to_load['schema2']]


    print("Loading files to raw.")
    stage_table_names = load(session=session, 
                                              files_to_load=files_to_load, 
                                              load_stage_names=load_stage_names, 
                                              load_table_name=load_table_name)    
    
    print("Transforming records to customers table.")
    trips_table_name = transform(session=session, 
                                       trips_table_name=trips_table_name)
    return trips_table_name