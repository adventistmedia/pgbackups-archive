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

  # For Rackspace
  SEGMENT_LIMIT = 5368709119.0  # 5GB -1
  BUFFER_SIZE = 1024 * 1024 # 1MB
  def store
    puts "Begin file upload [#{@file}]"
    begin
      bucket.files.create :key => @key, :body => @file, :public => false, :multipart_chunk_size => 5242880
    rescue Exception => e
      STDERR.puts "Problem uploading to S3!: #{e.message}"
      STDERR.puts e.backtrace.join("\n")
    end

    if rackspace_directory = get_rackspace_directory
      puts "Begin RACKSPACE file upload [#{@file}]"
      begin
        # old way. doesn't support files > 5GB
        #rackspace_directory.files.create(:key => @key, :body => @file, :multipart_chunk_size => 5242880)

        # New way supports files up to 5TB (only 1,000 5GB segments are allowed)
        service = connect_rackspace

        File.open(@file) do |f|
          segment = 0
          until f.eof?
            segment += 1
            offset = 0

            # upload segment to cloud files
            segment_suffix = segment.to_s.rjust(10, '0')
            service.put_object(ENV['RACKSPACE_CONTAINER_NAME'], "#{@file}/#{segment_suffix}", nil) do
              if offset <= SEGMENT_LIMIT - BUFFER_SIZE
                buf = f.read(BUFFER_SIZE).to_s
                offset += buf.size
                buf
              else
                ''
              end
            end
          end
        end

        # write manifest file
        service.put_object_manifest(ENV['RACKSPACE_CONTAINER_NAME'], @file, 'X-Object-Manifest' => "#{ENV['RACKSPACE_CONTAINER_NAME']}/#{@file}/")
      rescue Exception => e
        STDERR.puts "Problem uploading to Rackspace!: #{e.message}"
        STDERR.puts e.backtrace.join("\n")
      end
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
