module Discovery
  class Discovery
    def self.for_environment(environment)
      environment ||= "local"

      discoveryrc = File.expand_path("~/.discoveryrc")
      unless File.exist?(discoveryrc) then
        File.open(discoveryrc, "w"){ |f| f.puts("local = http://localhost:8080") }
      end

      value = File.readlines(discoveryrc).map { |l| l.strip.split(/\s*=\s*/, 2) }.select { |k, v| k == environment }.map { |k, v| v }.first
      raise "No such environment '#{environment}'" if value.nil?
      discovery_uris = value.split(/\s*,\s*/)
      return Discovery.new(discovery_uris)
    end

    def initialize(discovery_uris, client = HTTPClient.new)
      @client = client
      @discovery_uris = discovery_uris
    end

    def lookup(type, pool)
      service_descriptors = []
      @discovery_uris.each do |discovery_uri|
        begin
          url = "#{discovery_uri}/v1/service/#{type}/#{pool}"
          puts "Getting services at \"#{url}\"" if Verbose
          response = @client.get(url)
          if response.status == HTTP::Status::OK then
            body = response.body
            if body.is_a?(HTTP::Message::Body) then
              body = body.content
            end
            service_descriptors = JSON.parse(body)["services"]
            break
          end
        rescue => e
          # ignored
          p "ERROR:: #{e}" if Verbose
        end
      end

      if service_descriptors.nil? then
        return nil
      end

      return service_descriptors
    end
  end
end
