###### GEMINI IMAGE UNDERSTANDING #######################################################
# upload_blob_from_memory(
#     contents=message_content,
#     user_id=event.source.user_id,
#     message_id=event.message.id,
#     type="image",
# )

# json, image_description = gemini_describe_image(
#     user_id=event.source.user_id,
#     message_id=event.message.id,
# )

# if image_description:
#     line_bot_api.push_message(
#         PushMessageRequest(
#             to=event.source.user_id,
#             messages=[TextMessage(text=str(json))],
#         )
#     )
##########################################################################################
##########################################################################################


###### VERTEXT SEARCH FROM IMAGE  ##########

#     response_dict = vertex_search_retail_products(
#         image_description["product_description"]
#     )
#     build_flex_carousel_message(
#         line_bot_api=line_bot_api,
#         event=event,
#         response_dict=response_dict,
#         search_query=image_description["product_description"],
#         additional_explain=image_description["explaination"],
#     )
##########################################################################################