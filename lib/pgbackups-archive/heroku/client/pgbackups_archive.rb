require "heroku/client"
require "tmpdir"
require "gpgme"
require "rake"

class Heroku::Client::PgbackupsArchive

  attr_reader :client, :pgbackup

  def self.perform
    backup = new

    # If you're using the auto retention backups
    # or some other process
    # you can just ship the most recent backup
    if ENV['USE_LATEST_BACKUP']
      backup.use_latest_backup

    # Take a bespoke backup for this shipment
    else
      backup.capture

    end
    backup.download
    backup.encrypt
    backup.archive
    backup.delete

  rescue => e
    puts "PGBackup: #{@pgbackup.inspect}"

    raise e
  end

  def initialize(attrs={})
    Heroku::Command.load
    @client   = Heroku::Client::Pgbackups.new pgbackups_url
    @pgbackup = nil
  end

  def archive
    puts "Archive backup to S3 [#{key}]"
    PgbackupsArchive::Storage.new(key, file).store
  end

  def capture
    @pgbackup = @client.create_transfer(database_url, database_url, nil,
      "BACKUP", :expire => true)

    until @pgbackup["finished_at"]
      print "."
      sleep 1
      @pgbackup = @client.get_transfer @pgbackup["id"]
    end
  end

  def use_latest_backup
    @pgbackup = @client.get_latest_backup

    puts "Latest backup: [#{@pgbackup}]"
    if @pgbackup['created_at']
      created_at = DateTime.parse(@pgbackup['created_at'])
      hours_since_creation = (Time.now.to_i - created_at.to_time.to_i) / 3600
      puts "backup created #{hours_since_creation} hours ago"
      puts "LATEST_BACKUP_MORE_THAN_24HR_OLD #{hours_since_creation > 24}"

    else
      puts "CREATED_AT timestamp not available. Skipping backup age check."

    end

    @pgbackup
  end

  def delete
    if pgp_public_key
      File.delete pgp_temp_file
    end

    File.delete temp_file
  end

  def download
    puts "Download backup"
    File.open(temp_file, "wb") do |output|
      streamer = lambda do |chunk, remaining_bytes, total_bytes|
        output.write chunk
      end
      Excon.get(@pgbackup["public_url"], :response_block => streamer)
    end
  end

  def encrypt
    if public_key = pgp_public_key
      system "gpg -o #{pgp_temp_file} -r #{public_key.uids.first.email} -e #{temp_file}"
    end
  end

  private

  def database_url
    ENV["PGBACKUPS_DATABASE_URL"] || ENV["DATABASE_URL"]
  end

  def environment
    defined?(Rails) ? Rails.env : nil
  end

  def file
    if pgp_public_key
      File.open pgp_temp_file, "r"
    else
      File.open temp_file, "r"
    end
  end

  def key
    _key = ["pgbackups", environment, @pgbackup["finished_at"]
      .gsub(/\/|\:|\.|\s/, "-").concat(".dump")].compact.join("/")

    _key = _key + '.pgp' if pgp_public_key

    _key
  end

  def pgbackups_url
    ENV["PGBACKUPS_URL"]
  end

  def temp_file
    "#{Dir.tmpdir}/#{URI(@pgbackup['public_url']).path.split('/').last}"
  end

  def pgp_temp_file
    temp_file + '.pgp'
  end

  def pgp_public_key
    return nil unless ENV['KEY_DOMAIN'] and ENV['PGP_PUBLIC_KEY']

    public_keys = ::GPGME::Key.find :public, ENV['KEY_DOMAIN']

    # If the NCC key doesn't exist in the public key-chain then lets add it
    if public_keys.empty?
      puts "Importing Public Key into GPG Keychain"

      ::GPGME::Key.import(ENV['PGP_PUBLIC_KEY'])
      public_keys = ::GPGME::Key.find :public, ENV['KEY_DOMAIN']
    end

    public_keys.first
  end

end
