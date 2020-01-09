#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'

require 'google/apis/drive_v3'
require 'google/apis/errors'
require 'googleauth'
require 'googleauth/stores/file_token_store'
require 'fileutils'
require 'aws-sdk-s3'
require 'net/http'

FOLDER_NAME = ENV['DRIVE_FOLDER_NAME'].freeze
BUCKET_NAME = ENV['BUCKET_NAME'].freeze
SLACK_WEBHOOK = ENV['SLACK_WEBHOOK'].freeze
CHANNEL = ENV['SLACK_CHANNEL'].freeze
FILE_LIMIT = ENV['FILE_LIMIT'].freeze

OOB_URI = 'urn:ietf:wg:oauth:2.0:oob'.freeze
APPLICATION_NAME = 'test-drive-api'.freeze
CREDENTIALS_PATH = 'credentials/credentials.json'.freeze
# The file token.yaml stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
TOKEN_PATH = 'credentials/token.yaml'.freeze
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
  user_id = 'default'
  credentials = authorizer.get_credentials user_id
  if credentials.nil?
    url = authorizer.get_authorization_url base_url: OOB_URI
    puts 'Open the following URL in the browser and enter the ' \
         "resulting code after authorization:\n" + url
    code = gets
    credentials = authorizer.get_and_store_credentials_from_code(
      user_id: user_id, code: code, base_url: OOB_URI
    )
  end
  credentials
end

def initiate_drive
  drive_service = Google::Apis::DriveV3::DriveService.new
  drive_service.client_options.application_name = APPLICATION_NAME
  drive_service.authorization = authorize
  drive_service
end

def send_slack_notification(file_name:, file_id:)
  slack_text = "new file uploaded, name: #{file_name}, id: #{file_id}"
  uri = URI(SLACK_WEBHOOK)
  params = { text: slack_text, channel: CHANNEL, username: 'Notifier bot' }
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  request = Net::HTTP::Post.new(uri)
  request.body = params.to_json

  http.request(request)
end

def get_drive_files(foldername:, limit: 20)
  drive_service = initiate_drive

  query = "mimeType='application/vnd.google-apps.folder' and name='#{foldername}'"
  folder = drive_service.list_files(q: query, spaces: 'drive')
  folder_id = folder.files[0].id

  query = "'#{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
  fields = 'files(id, name, created_time)'
  files = drive_service.list_files(q: query, fields: fields).files

  # newest files, up to FILE_LIMIT, default 20
  files.sort{ |file| file.created_time}
  files.take(limit.to_i)
end

def get_s3_files
  s3 = Aws::S3::Client.new
  s3.list_objects_v2(
    bucket: BUCKET_NAME,
    max_keys: FILE_LIMIT
  )
end

def upload_s3_file(id:)
  s3 = Aws::S3::Client.new
  s3.put_object(
    bucket: BUCKET_NAME,
    key: id.to_s,
    server_side_encryption: 'AES256',
    storage_class: 'STANDARD_IA'
  )
end

drive_files = get_drive_files(foldername: FOLDER_NAME, limit: FILE_LIMIT)
s3_files = get_s3_files
ids = s3_files.contents.map(&:key)
# pp drive_files
puts '----------------------------------------------------'

drive_files.map do |file|
  name = file.name
  id = file.id
  # puts "name: #{name} - created at: #{file.created_time} - id: #{id}"
  unless ids.include?(id)
    upload_s3_file(id: id)
    send_slack_notification(file_name: name, file_id: id)
  end
end
