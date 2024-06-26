﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching;
using Enyim.Caching.SampleWebApp;
using Enyim.Caching.SampleWebApp.Controllers;
using Enyim.Caching.SampleWebApp.Models;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace SampleWebApp.IntegrationTests
{
    public class HomeControllerTests : IClassFixture<WebApplicationFactory<Startup>>
    {
        private readonly WebApplicationFactory<Startup> _factory;

        public HomeControllerTests(WebApplicationFactory<Startup> factory)
        {
            _factory = factory;
        }

        [Fact]
        public async Task HomeController_Index()
        {
            var httpClient = _factory.CreateClient();
            var response = await httpClient.GetAsync("/");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var memcachedClient = _factory.Server.Host.Services.GetRequiredService<IMemcachedClient>();
            var postsDict = await memcachedClient.GetValueAsync<Dictionary<string, List<BlogPost>>>(HomeController.CacheKey);
            Assert.NotNull(postsDict);
            Assert.NotEmpty(postsDict.First().Value.First().Title);

            await memcachedClient.RemoveAsync(HomeController.CacheKey);
        }

        [Fact]
        public async Task Get_postbody_from_cache_ok()
        {
            var httpClient = _factory.CreateClient();
            var response = await httpClient.GetAsync("/home/postbody");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
    }
}
