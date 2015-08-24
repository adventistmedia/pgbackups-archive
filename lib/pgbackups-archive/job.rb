require "heroku/command/pg"
require "heroku/command/pg_backups"
require "heroku/api"
require "tmpdir"

class PgbackupsArchive::Job

  attr_reader :client
  attr_accessor :backup_url, :created_at

  def self.call
    new.call
  end

  def initialize(attrs={})
    Heroku::Command.load
    @client = Heroku::Command::Pg.new([], :app => ENV["PGBACKUPS_APP"])
  end

  def call

    if ENV['USE_LATEST_BACKUP']
      use_latest_backup

    # Take a bespoke backup for this shipment
    else
      expire
      capture

    end

    download
    encrypt
    archive
    delete
  end

  def archive
    if PgbackupsArchive::Storage.new(key, file).store
      client.display "Backup archived"
    end
  end

  def use_latest_backup
    backup = get_latest_backup

    puts "Latest backup: [#{backup}]"
    if backup[:created_at]
      created_at = DateTime.parse(backup[:created_at])
      hours_since_creation = (Time.now.to_i - created_at.to_time.to_i) / 3600
      puts "backup created #{hours_since_creation} hours ago"
      puts "LATEST_BACKUP_MORE_THAN_24HR_OLD #{hours_since_creation > 24}"

    else
      puts "CREATED_AT timestamp not available. Skipping backup age check."

    end

    use_backup( backup )
  end

  def capture
    attachment = client.send(:generate_resolver).resolve(database)
    backup = client.send(:hpg_client, attachment).backups_capture
    client.send(:poll_transfer, "backup", backup[:uuid])

    use_backup( backup )
  end

  def delete
    File.delete(temp_file)
  end

  def download
    File.open(temp_file, "wb") do |output|
      streamer = lambda do |chunk, remaining_bytes, total_bytes|
        output.write chunk
      end

      # https://github.com/excon/excon/issues/475
      Excon.get backup_url,
        :response_block    => streamer,
        :omit_default_port => true
    end
  end

  def encrypt
    if pgp_public_key
      system "gpg --trust-mode always -o #{pgp_temp_file} -r #{ENV['KEY_EMAIL']} -e #{temp_file}"
    end
  end

  def expire
    transfers = client.send(:hpg_app_client, ENV["PGBACKUPS_APP"]).transfers
      .select  { |b| b[:from_type] == "pg_dump" && b[:to_type] == "gof3r" }
      .sort_by { |b| b[:created_at] }

    if transfers.size > pgbackups_to_keep
      backup_id  = "b%03d" % transfers.first[:num]
      backup_num = client.send(:backup_num, backup_id)

      expire_backup(backup_num)

      client.display "Backup #{backup_id} expired"
    end
  end

  private

  def use_backup(backup)
    self.created_at = backup[:created_at]

    self.backup_url = Heroku::Client::HerokuPostgresqlApp
      .new(ENV["PGBACKUPS_APP"]).transfers_public_url(backup[:num])[:url]
  end

  def get_latest_backup
    transfers = client.send(:hpg_app_client, ENV["PGBACKUPS_APP"]).
                       transfers.sort_by { |b| b[:created_at] }

    latest_transfer = transfers.last

    latest_transfer
  end

  def expire_backup(backup_num)
    client.send(:hpg_app_client, ENV["PGBACKUPS_APP"])
      .transfers_delete(backup_num)
  end

  def database
    ENV["PGBACKUPS_DATABASE"] || "DATABASE_URL"
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
    timestamp = created_at.gsub(/\/|\:|\.|\s/, "-").concat(".dump")
    _key = ["pgbackups", environment, timestamp].compact.join("/")

    _key = _key + '.pgp' if pgp_public_key

    _key
  end

  def pgbackups_to_keep
    var = ENV["PGBACKUPS_KEEP"] ? var.to_i : 30
  end

  def temp_file
    "#{Dir.tmpdir}/pgbackup"
  end

  def pgp_temp_file
    temp_file + '.pgp'
  end

  def pgp_public_key
    return nil unless ENV['KEY_EMAIL'] and ENV['PGP_PUBLIC_KEY']

    puts "Looking for #{ENV['KEY_EMAIL']} public key"
    found_public_keys = system "gpg --list-keys #{ENV['KEY_EMAIL']}"


    # If the key doesn't exist in the public key-chain then lets add it
    if !found_public_keys
      puts "Importing Public Key into GPG Keychain"

      # Write ENV['PGP_PUBLIC_KEY'] to disk
      File.open('/tmp/public.key', 'w') do |f|
        f.write(ENV['PGP_PUBLIC_KEY'])
      end

      system "gpg --import '/tmp/public.key'"
      found_public_keys = system "gpg --list-keys #{ENV['KEY_EMAIL']}"
    end

    found_public_keys
  end

end
