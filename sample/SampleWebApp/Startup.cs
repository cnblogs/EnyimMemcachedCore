using Enyim.Caching.SampleWebApp.Models;
using Enyim.Caching.SampleWebApp.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Enyim.Caching.SampleWebApp
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddEnyimMemcached();
            services.AddEnyimMemcached<PostBody>(Configuration, "postbodyMemcached");
            //services.AddEnyimMemcached(Configuration);
            //services.AddEnyimMemcached(Configuration, "enyimMemcached");
            //services.AddEnyimMemcached(Configuration.GetSection("enyimMemcached"));
            services.AddTransient<IBlogPostService, BlogPostService>();
            services.AddMvc();
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseEnyimMemcached();

            app.UseRouting();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapDefaultControllerRoute();
            });
        }
    }
}
