using Azure;
using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;


namespace TwinFunctions
{

    public class TwinsFunction
    {
        //Your Digital Twin URL is stored in an application setting in Azure Functions
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("TwinsFunction")]
        public async void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation(eventGridEvent.Data.ToString());
            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
            try
            {
                //Authenticate with Digital Twins
                ManagedIdentityCredential cred = new ManagedIdentityCredential("https://digitaltwins.azure.net");
                DigitalTwinsClient client = new DigitalTwinsClient(new Uri(adtInstanceUrl), cred, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(httpClient) });
                log.LogInformation($"ADT service client connection created.");
                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                    // Reading deviceId and temperature for IoT Hub JSON
                    JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                    string deviceId = (string)deviceMessage["systemProperties"]["iothub-connection-device-id"];
                    string deviceType = (string)deviceMessage["body"]["DeviceType"];
                    log.LogInformation($"Device:{deviceId} DeviceType is:{deviceType}");
                    switch (deviceType){
                        case "Gearbox":
                        var gearboxPayload = new Dictionary<string, int>
                        {
                            ["GearInjNozzleTemp"] = deviceMessage["body"]["GearInjNozzleTemp"].Value<int>(),
                            ["GearPump2Rpm"] = deviceMessage["body"]["GearPump2Rpm"].Value<int>(),
                            ["GearLubricationPressure"] = deviceMessage["body"]["GearLubricationPressure"].Value<int>(),
                            ["GearOilPressure"] = deviceMessage["body"]["GearOilPressure"].Value<int>(),
                            ["GearOilTankLevel"] = deviceMessage["body"]["GearOilTankLevel"].Value<int>()
                        };
                            await client.PublishTelemetryAsync(deviceId, Guid.NewGuid().ToString(), JsonConvert.SerializeObject(gearboxPayload));
                            log.LogInformation($"Published component telemetry message to twin '{deviceId}'.");
                        break;
                        case "Generator":
                        var generatorPayload = new Dictionary<string, int>
                        {
                            ["generatingVoltage"] = deviceMessage["body"]["generatingVoltage"].Value<int>(),
                            ["generatingPower"] = deviceMessage["body"]["generatingPower"].Value<int>(),
                            ["GeneratorCableTemp"] = deviceMessage["body"]["GeneratorCableTemp"].Value<int>(),
                            ["GeneratorFanMotorRpm"] = deviceMessage["body"]["GeneratorFanMotorRpm"].Value<int>(),
                            ["GeneratorRpm"] = deviceMessage["body"]["GeneratorRpm"].Value<int>(),
                            ["GeneratorWaterTemp"] = deviceMessage["body"]["GeneratorWaterTemp"].Value<int>(),
                            ["GeneratorWaterOutPressure"] = deviceMessage["body"]["GeneratorWaterOutPressure"].Value<int>(),
                            ["WindingL1Inv1Temp"] = deviceMessage["body"]["WindingL1Inv1Temp"].Value<int>(),
                            ["WindingL2Inv1Temp"] = deviceMessage["body"]["WindingL2Inv1Temp"].Value<int>(),
                            ["WindingL3Inv1Temp"] = deviceMessage["body"]["WindingL3Inv1Temp"].Value<int>()
                        };
                            await client.PublishTelemetryAsync(deviceId, Guid.NewGuid().ToString(), JsonConvert.SerializeObject(generatorPayload));
                            log.LogInformation($"Published component telemetry message to twin '{deviceId}'.");
                        break;

                    }

                }
            }
            catch (Exception e)
            {
                log.LogError(e.Message);
            }

        }
    }

}