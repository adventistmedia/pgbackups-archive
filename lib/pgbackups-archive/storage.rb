require "fog"
require "open-uri"

class PgbackupsArchive::Storage

  def initialize(key, file)
    @key = key
    @file = file
  end

  def connection
    Fog::Storage.new({
      :provider              => "AWS",
      :aws_access_key_id     => ENV["PGBACKUPS_AWS_ACCESS_KEY_ID"],
      :aws_secret_access_key => ENV["PGBACKUPS_AWS_SECRET_ACCESS_KEY"],
      :region                => ENV["PGBACKUPS_REGION"],
      :persistent            => false
    })
  end

  def bucket
    puts "Open bucket: [#{ENV["PGBACKUPS_BUCKET"]}]"
    connection.directories.get ENV["PGBACKUPS_BUCKET"]
  end

  def store
    puts "Begin file upload [#{@file}]"
    bucket.files.create :key => @key, :body => @file, :public => false, :multipart_chunk_size => 5242880

    if rackspace_directory = get_rackspace_directory
      puts "Begin RACKSPACE file upload [#{@file}]"
      rackspace_directory.files.create(:key => @key, :body => @file, :multipart_chunk_size => 5242880)
    end

  end

  def get_rackspace_directory
    return nil unless api = connect_rackspace

    puts "Open Rackspace directory: #{ENV['RACKSPACE_CONTAINER_NAME']}"
    dirs = api.directories
    dir = dirs.select { |d| d.key == ENV['RACKSPACE_CONTAINER_NAME'] }.last
  end

  def connect_rackspace
    return nil unless ENV['RACKSPACE_USER_NAME'] and ENV['RACKSPACE_API'] and ENV['RACKSPACE_CONTAINER_NAME']

    service = Fog::Storage.new({
        :provider            => 'Rackspace',         # Rackspace Fog provider
        :rackspace_username  => ENV['RACKSPACE_USER_NAME'], # Your Rackspace Username
        :rackspace_api_key   => ENV['RACKSPACE_API'],       # Your Rackspace API key
        :rackspace_region    => :ord,                # Defaults to :dfw
        :connection_options  => {}                   # Optional
    })
  end

end
