# Not used by IoW; here for reference


# @op(ins={"start": In(Nothing)})
# def nabu_prov_drain(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")
#     returned_value = run_gleaner(context, "prov-drain", source)
#     r = str("returned value:{}".format(returned_value))
#     get_dagster_logger().info(f"nabu prov-drain returned  {r} ")
# @op(ins={"start": In(Nothing)})
# def missingreport_s3(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")
#     source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
#     source_url = source.get("url")
#     s3Minio = s3.MinioDatastore(
#         _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
#     )
#     bucket = GLEANER_MINIO_BUCKET
#     source_name = source
#     graphendpoint = None
#     milled = False
#     summon = True
#     returned_value = missingReport(
#         source_url,
#         bucket,
#         source_name,
#         s3Minio,
#         graphendpoint,
#         milled=milled,
#         summon=summon,
#     )
#     r = str("missing repoort returned value:{}".format(returned_value))
#     report = json.dumps(returned_value, indent=2)
#     s3Minio.putReportFile(bucket, source_name, "missing_report_s3.json", report)
#     get_dagster_logger().info(f"missing s3 report  returned  {r} ")


# @op(ins={"start": In(Nothing)})
# def missingreport_graph(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")
#     source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
#     source_url = source.get("url")
#     s3Minio = s3.MinioDatastore(
#         _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
#     )
#     bucket = GLEANER_MINIO_BUCKET
#     source_name = source

#     graphendpoint = _graphEndpoint()

#     milled = True
#     summon = False  # summon only off
#     returned_value = missingReport(
#         source_url,
#         bucket,
#         source_name,
#         s3Minio,
#         graphendpoint,
#         milled=milled,
#         summon=summon,
#     )
#     r = str("missing report graph returned value:{}".format(returned_value))
#     report = json.dumps(returned_value, indent=2)

#     s3Minio.putReportFile(bucket, source_name, "missing_report_graph.json", report)
#     get_dagster_logger().info(f"missing graph  report  returned  {r} ")


# @op(ins={"start": In(Nothing)})
# def graph_reports(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")

#     source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
#     s3Minio = s3.MinioDatastore(
#         _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
#     )
#     bucket = GLEANER_MINIO_BUCKET
#     source_name = source

#     graphendpoint = _graphEndpoint()

#     returned_value = generateGraphReportsRepo(
#         source_name, graphendpoint, reportList=reportTypes["repo_detailed"]
#     )
#     r = str("returned value:{}".format(returned_value))
#     # report = json.dumps(returned_value, indent=2) # value already json.dumps
#     report = returned_value
#     s3Minio.putReportFile(bucket, source_name, "graph_stats.json", report)
#     get_dagster_logger().info(f"graph report  returned  {r} ")


# @op(ins={"start": In(Nothing)})
# def identifier_stats(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")

#     source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
#     s3Minio = s3.MinioDatastore(
#         _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
#     )
#     bucket = GLEANER_MINIO_BUCKET
#     source_name = source

#     returned_value = generateIdentifierRepo(source_name, bucket, s3Minio)
#     r = str("returned value:{}".format(returned_value))
#     # r = str('identifier stats returned value:{}'.format(returned_value))
#     report = returned_value.to_json()
#     s3Minio.putReportFile(bucket, source_name, "identifier_stats.json", report)
#     get_dagster_logger().info(f"identifer stats report  returned  {r} ")


# @op(ins={"start": In(Nothing)})
# def bucket_urls(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")

#     s3Minio = s3.MinioDatastore(
#         f"{GLEANER_MINIO_ADDRESS}:{GLEANER_MINIO_PORT}", MINIO_OPTIONS
#     )
#     bucket = GLEANER_MINIO_BUCKET
#     source_name = source

#     res = s3Minio.listSummonedUrls(bucket, source_name)
#     bucketurls = json.dumps(res, indent=2)
#     s3Minio.putReportFile(
#         GLEANER_MINIO_BUCKET, source_name, "bucketutil_urls.json", bucketurls
#     )
#     get_dagster_logger().info(f"bucker urls report  returned value: {res} ")


# @op(ins={"start": In(Nothing)})
# def summarize(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")

#     s3Minio = s3.MinioDatastore(
#         _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
#     )
#     bucket = GLEANER_MINIO_BUCKET
#     source_name = source
#     endpoint = _graphEndpoint()  # getting data, not uploading data
#     summary_namespace = _graphSummaryEndpoint()

#     try:
#         summarydf = get_summary4repoSubset(endpoint, source_name)
#         nt, g = summaryDF2ttl(summarydf, source_name)  # let's try the new generator
#         summaryttl = g.serialize(format="longturtle")
#         # Lets always write out file to s3, and insert as a separate process
#         # we might be able to make this an asset..., but would need to be acessible by http
#         # if not stored in s3
#         objectname = f"{SUMMARY_PATH}/{source_name}_release.ttl"  # needs to match that is expected by post

#         s3Minio.putTextFileToStore(summaryttl, S3ObjectInfo(bucket, objectname))
#         # inserted = sumnsgraph.insert(bytes(summaryttl, 'utf-8'), content_type="application/x-turtle")
#         # if not inserted:
#         #    raise Exception("Loading to graph failed.")
#     except Exception as e:
#         # use dagster logger
#         get_dagster_logger().error(f"Summary. Issue creating graph  {str(e)} ")
#         raise Exception(f"Loading Summary graph failed. {str(e)}")


# @op(ins={"start": In(Nothing)})
# def upload_summarize(context: OpExecutionContext):
#     source = strict_get_tag(context, "source")
#     returned_value = post_to_graph(
#         source,
#         path=SUMMARY_PATH,
#         extension="ttl",
#         graphendpoint=_graphSummaryEndpoint(),
#     )
#     # the above can be done (with a better path approach) in Nabu
#     # returned_value = gleanerio(context, ("object"), source)
#     r = str("returned value:{}".format(returned_value))
#     get_dagster_logger().info(f"upload summary returned  {r} ")