#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'

require 'google/apis/drive_v3'
require 'google/apis/errors'
require "googleauth"
require "googleauth/stores/file_token_store"
require "fileutils"
require 'aws-sdk-s3'
require 'net/http'

OOB_URI = "urn:ietf:wg:oauth:2.0:oob".freeze
APPLICATION_NAME = "test-drive-api".freeze
CREDENTIALS_PATH = "credentials/credentials.json".freeze
# The file token.yaml stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
TOKEN_PATH = "credentials/token.yaml".freeze
SCOPE = Google::Apis::DriveV3::AUTH_DRIVE_METADATA_READONLY

##
# Ensure valid credentials, either by restoring from the saved credentials
# files or intitiating an OAuth2 authorization. If authorization is required,
# the user's default browser will be launched to approve the request.
#
# @return [Google::Auth::UserRefreshCredentials] OAuth2 credentials
def authorize
  client_id = Google::Auth::ClientId.from_file CREDENTIALS_PATH
  token_store = Google::Auth::Stores::FileTokenStore.new file: TOKEN_PATH
  authorizer = Google::Auth::UserAuthorizer.new client_id, SCOPE, token_store
  user_id = "default"
  credentials = authorizer.get_credentials user_id
  if credentials.nil?
    url = authorizer.get_authorization_url base_url: OOB_URI
    puts "Open the following URL in the browser and enter the " \
         "resulting code after authorization:\n" + url
    code = gets
    credentials = authorizer.get_and_store_credentials_from_code(
      user_id: user_id, code: code, base_url: OOB_URI
    )
  end
  credentials
end

def send_slack_notification(file_name, file_id)
  slack_text = "new voicememo uploaded, name: #{file_name}, id: #{file_id}"
  uri = URI('https://hooks.slack.com/services/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')
  params = {text: slack_text, channel: "#voicememos-test", username: "SessionBot"}
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  request = Net::HTTP::Post.new(uri)
  request.body = params.to_json

  response = http.request(request)
end

drive_service = Google::Apis::DriveV3::DriveService.new
drive_service.client_options.application_name = APPLICATION_NAME
drive_service.authorization = authorize
folder_name = 'voicememos'

folder = drive_service.list_files(q: "mimeType='application/vnd.google-apps.folder' and name='#{folder_name}'", spaces: 'drive')
folder_id = nil
folder.files.map { |file| folder_id = file.id  }

voicememos = drive_service.list_files(q: "'#{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false", fields: "files(id, name, created_time)").files

# limit voicememos to newest 50  
voicememos.sort{ |memo| memo.created_time}
voicememos = voicememos.take(50)

s3 = Aws::S3::Client.new
resp = s3.list_objects_v2({
  bucket: "voicememo-notifications", 
  max_keys: 50, 
})
# pp resp.to_h
ids = resp.contents.map { |object| object.key }
puts '----------------------------------------------------'

voicememos.map do |file| 
  name = file.name
  id = file.id
  # puts "name: #{name} - created at: #{file.created_time} - id: #{id}"
  # TODO check what errors s3.put_object throws
  if (!ids.include?(id))
    resp = s3.put_object({
      bucket: "voicememo-notifications", 
      key: id.to_s, 
      server_side_encryption: "AES256", 
      storage_class: "STANDARD_IA", 
    })
    send_slack_notification(name, id)
  end
end

